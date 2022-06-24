from ftplib import FTP
import os
import pandas as pd
import numpy as np
from pathlib import Path
import configparser
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")
config_db = dict(
    drivername="mariadb",
    username=alg_config.get("database leveranciers", "user"),
    password=alg_config.get("database leveranciers", "password"),
    host=alg_config.get("database leveranciers", "host"),
    port=alg_config.get("database leveranciers", "port"),
    database=alg_config.get("database leveranciers", "database"),
)
engine = create_engine(URL.create(**config_db))

def get_latest_file():
    with FTP(host=alg_config.get("difox ftp", "server")) as ftp:
        ftp.login(user=alg_config.get("difox ftp", "user"), passwd=alg_config.get("difox ftp", "passwd"))

        # ftp.retrlines('LIST')

        names = ftp.nlst()
        final_names = [line for line in names if "difox" in line]

        latest_time = None
        latest_name = None

        for name in final_names:
            time = ftp.sendcmd("MDTM " + name)
            if (latest_time is None) or (time > latest_time):
                latest_name = name
                latest_time = time

        with open(latest_name, "wb") as f:
            ftp.retrbinary("RETR " + latest_name, f.write)


get_latest_file()

benodigde_kolomen = ["artikelnr.", "AEN-code 1", "Beschikbaarheid ( in stappen)"]

difox = (
    pd.read_csv(
        max(Path.cwd().glob("difox*.CSV"), key=os.path.getctime),
        sep=";",
        usecols=benodigde_kolomen,
        dtype={"AEN-code 1": object},
    )
    .rename(
        columns={
            "artikelnr.": "sku",
            "AEN-code 1": "ean",
            "Beschikbaarheid ( in stappen)": "stock",
        }
    )
    .assign(
        stock=lambda x: (np.where(pd.to_numeric(x["stock"].str.replace("> ", "").fillna(0)) > 6, 6, x["stock"])).astype(
            float
        ),  # niet teveel aanbieden
        sku=lambda x: "DIF" + x["sku"].astype(str),
        ean = lambda x: pd.to_numeric(x["ean"], errors="coerce")
    )
    .query("ean == ean")
)

date_now = datetime.now().strftime("%c").replace(":", "-")

difox.to_csv("~/DIF/actueel/DIF_actueele_voorraad_" + date_now + ".csv", index=False, encoding="utf-8-sig")

difox_database = difox[
    [
        "sku",
        "ean",
        "stock",
    ]
]

difox.to_sql("DIF_voorraad", con=engine, if_exists="replace", index=False, chunksize=1000)


with engine.connect() as con:

    aantal_items = con.execute("SELECT count(*) FROM DIF_voorraad").fetchall()[-1][-1]
    totaal_stock = int(con.execute("SELECT sum(stock) FROM DIF_voorraad").fetchall()[-1][-1])
    leverancier = "DIF_voorraad"
    sql_insert = "INSERT INTO process_import_log_voorraad (aantal_items, totaal_stock, leverancier) VALUES (%s,%s,%s)"
    con.execute(sql_insert, (aantal_items, totaal_stock, leverancier))

engine.dispose()
