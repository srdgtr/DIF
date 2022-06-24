import configparser
import os
import sys
from datetime import datetime
from ftplib import FTP
from pathlib import Path

import dropbox
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

sys.path.insert(0, str(Path.home()))
from bol_export_file import get_file

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
current_folder = Path.cwd().name.upper()
export_config = configparser.ConfigParser(interpolation=None)
export_config.read(Path.home() / "bol_export_files.ini")
korting_percent = int(export_config.get("stap 1 vaste korting", current_folder.lower()).strip("%"))
dbx_api_key = alg_config.get("dropbox", "api_dropbox")
dbx = dropbox.Dropbox(dbx_api_key)


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

difox = (
    pd.read_csv(
        max(Path.cwd().glob("difox*.CSV"), key=os.path.getctime),
        sep=";",
        dtype={
            "AEN-code 1": object,
        },
    )
    .rename(
        columns={
            "artikelnr.": "sku",
            "AEN-code 1": "ean",
            "inStock": "stock",
            "naam fabrikant": "brand",
            "prijs": "price",
            "Beschikbaarheid ( in stappen)": "stock",
            "catalogusprijs": "price_advice",
            "artikelomschrijving": "info",
            "artikelgroep naam": "group",
            "artikelnummer fabrikant": "id",
            "Sonderversandkosten": "pallet",
            "Combined length and girth exceeded":"tezwaar_exta18",
            "Battery Watt-hour":"cap_battery",
        }
    )
    .assign(
        stock=lambda x: (np.where(pd.to_numeric(x["stock"].str.replace("> ", "").fillna(0)) > 6, 6, x["stock"])).astype(
            float
        ),  # niet teveel aanbieden
        eigen_sku=lambda x: "DIF" + x["sku"].astype(str),
        ean = lambda x: pd.to_numeric(x["ean"], errors="coerce"),
        price = lambda x: (np.where((x["tezwaar_exta18"] == 1) | (x["cap_battery"] >= 100),x["price"]+20,x["price"])), #extra bij te zwaar/groot of zware battery
        gewicht="",
        url_artikel="",
        lange_omschrijving="",
        verpakings_eenheid="",
        lk=lambda x: (korting_percent * x["price"] / 100).round(2),
    ).assign(price=lambda x: (x["price"] - x["lk"]).round(2))
    .query("stock > 0")
    .query("ean == ean")
)

difox_basis = difox[
    ["sku", "ean", "brand", "stock", "price", "price_advice", "info", "group", "id", "pallet","tezwaar_exta18","cap_battery","lk"]
]
date_now = datetime.now().strftime("%c").replace(":", "-")

difox_basis.to_csv("DIF_full_bol_descriptions_" + date_now + ".csv", index=False, encoding="utf-8-sig")

dif_info = difox.rename(
    columns={
        "price": "prijs",
        "brand": "merk",
        "price_advice": "advies_prijs",
        "group": "category",
        "info": "product_title",
        "stock": "voorraad",
    }
)

latest_dif_file = max(Path.cwd().glob("DIF_full_*.csv"), key=os.path.getctime)
with open(latest_dif_file, "rb") as f:
    dbx.files_upload(
        f.read(), "/macro/datafiles/DIF/" + latest_dif_file.name, mode=dropbox.files.WriteMode("overwrite", None), mute=True
    )

dif_info_db = dif_info[
    [
        "eigen_sku",
        "sku",
        "ean",
        "voorraad",
        "merk",
        "prijs",
        "advies_prijs",
        "category",
        "gewicht",
        "url_artikel",
        "product_title",
        "lange_omschrijving",
        "verpakings_eenheid",
        "lk"
    ]
]

huidige_datum = datetime.now().strftime("%d_%b_%Y")
dif_info_db.to_sql(f"{current_folder}_dag_{huidige_datum}", con=engine, if_exists="replace", index=False, chunksize=1000)

with engine.connect() as con:
    con.execute(f"ALTER TABLE {current_folder}_dag_{huidige_datum} ADD PRIMARY KEY (eigen_sku(20))")
    aantal_items = con.execute(f"SELECT count(*) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
    totaal_stock = int(con.execute(f"SELECT sum(voorraad) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1])
    totaal_prijs = int(con.execute(f"SELECT sum(prijs) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1])
    leverancier = f"{current_folder}"
    sql_insert = (
        "INSERT INTO process_import_log (aantal_items, totaal_stock, totaal_prijs, leverancier) VALUES (%s,%s,%s,%s)"
    )
    con.execute(sql_insert, (aantal_items, totaal_stock, totaal_prijs, leverancier))

engine.dispose()
