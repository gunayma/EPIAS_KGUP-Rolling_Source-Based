# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 12:03:20 2021

@author: mehmetg
"""

import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import requests
import json
import numpy as np

master_start_dt = datetime(2015,1,1)
x=datetime(2021,2,3)
today = datetime.today()
save_path = r"C:\Users\mehmetg\jupyter\EPIAS_KGUP-Rolling_Source-Based"

api_service_url = "https://seffaflik.epias.com.tr/transparency/service/production/dpp"
production_columns_map = {
    "fueloil": "FuelOil",
    "fuelOil": "FuelOil",
    "gasOil": "GasOil",
    "blackCoal": "BlackCoal",
    "tasKomur": "BlackCoal",
    "lignite": "Lignite",
    "geothermal": "Geothermal",
    "jeotermal": "Geothermal",
    "naturalGas": "NaturalGas",
    "dogalgaz": "NaturalGas",
    "river": "Run-of-River",
    "akarsu": "Run-of-River",
    "ruzgar": "Wind",
    "linyit": "Lignite",
    "dammedHydro": "Dam",
    "barajli": "Dam",
    "lng": "LNG",
    "biomass": "Biomass",
    "biokutle": "Biomass",
    "naphta": "Naphta",
    "nafta": "Naphta",
    "importCoal": "HardCoal",
    "ithalKomur": "HardCoal",
    "asphaltiteCoal": "Asphaltite",
    "wind": "Wind",
    "nucklear": "Nuclear",
    "sun": "Solar",
    "importExport": "ImportExport",
    "total": "Total",
    "diger": "Other",
    "toplam": "Total"
}

def get_planned_production(start_date, end_date):
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    payload = {
        "startDate": start_date_str,
        "endDate": end_date_str
    }
    r = requests.get(api_service_url, params=payload)
    r = json.loads(r.text)['body']['dppList']
    df = pd.DataFrame(r)
    if "saat" in df.columns:
        df = df.drop("saat", axis=1)
        
    df = df.set_index("tarih")
    df.index =  pd.to_datetime(df.index.map(lambda dt: str(dt)[:19]))
    df.columns = [production_columns_map[col] for col in df.columns]
    df.index.name = "DateTime"
    return df

year_dts = []
year_dts.append(master_start_dt)
while year_dts[-1] < today:
    year_dts.append(year_dts[-1] + relativedelta(years=1))
    
existing = os.listdir(save_path)
existing = [f for f in existing if '.csv' in f]
if len(existing) > 0:
    existing.sort()
    latest = int(existing[-1].split(".")[0])
    year_dts = [dt for dt in year_dts if dt.year >= latest]

for y in range(len(year_dts)-1):
    dt_start = year_dts[y]
    dt_end = year_dts[y+1] - timedelta(hours=1)
    print("Processing", dt_start.year)
    filename = "{}.csv".format(dt_start.year)
    file_save_path = os.path.join(save_path, filename)
    if os.path.exists(file_save_path):
        dummy = pd.read_csv(file_save_path, index_col=0, parse_dates=True)
        last_idx = dummy.index[-1]
        if last_idx != dt_end:
            print("Appending to", filename)
            new_dt_start = last_idx - timedelta(days=7) # start from last 7 days
            df = get_planned_production(new_dt_start, dt_end)
            df = pd.concat([dummy, df])
            df = df.groupby(df.index).last()
            df.to_csv(file_save_path)
    else:
        df = get_planned_production(dt_start, dt_end)
        df.to_csv(file_save_path)