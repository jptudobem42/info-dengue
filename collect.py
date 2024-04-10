# Databricks notebook source
from datetime import datetime
import time
import json
from pyspark.sql import SparkSession
import requests

# COMMAND ----------

class Collector:
    def __init__(self, url):
        self.spark = SparkSession.builder.appName("Collector").getOrCreate()
        self.url = url

    def get_municipios(self):
        query = "SELECT cod_municipio FROM codigos_ibge_municipios"
        df = self.spark.sql(query)
        return [row.cod_municipio for row in df.collect()]
    
    def save_json(self, data, disease, cod_municipio, year, week):
        file_path = f"/dbfs/mnt/datalake/info-dengue/raw/{disease}/{cod_municipio}-{year}{week}.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

    def get_and_save(self, disease, cod_municipio, year, week):
        params = {
            "geocode": cod_municipio,
            "disease": disease,
            "format": "json",
            "ew_start": str(week),
            "ew_end": str(week),
            "ey_start": str(year),
            "ey_end": str(year),
        }
        response = requests.get(self.url, params=params)

        if response.status_code == 200:
            try:
                data = response.json()
                self.save_json(data, disease, cod_municipio, year, week)
                return data
            except json.JSONDecodeError:
                print(f"Erro ao decodificar JSON: {disease}, {cod_municipio}, {year}, semana {week}")
                return None
        else:
            print(f"Request sem sucesso: {response.status_code}")
            return None

    def get_current_week(self):
        return datetime.now().isocalendar().week

    def collect_data(self, diseases, year_start, year_end):
        municipios = self.get_municipios()
        for disease in diseases:
            for year in range(int(year_start), int(year_end) + 1):
                last_week = self.get_current_week() if year == datetime.now().year else 53
                for week in range(1, last_week + 1):
                    for cod_municipio in municipios:
                        print(f"Coletando {disease} para {cod_municipio} no ano {year}, semana {week}...")
                        data = self.get_and_save(disease, cod_municipio, year, week)
                        if data is not None:
                            print(f"Coleta concluída para {disease} em {cod_municipio}, ano {year}, semana {week}.")
                        time.sleep(30)

# COMMAND ----------

url = "https://info.dengue.mat.br/api/alertcity"
diseases = ["dengue", "zika", "chikungunya"]
year_start = '2024'
year_end = str(datetime.now().year)

collector = Collector(url)
collector.collect_data(diseases, year_start, year_end)
