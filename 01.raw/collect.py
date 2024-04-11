# Databricks notebook source
from datetime import datetime
import time
import json
from pyspark.sql import SparkSession, functions as F
import requests

# COMMAND ----------

class Collector:
    def __init__(self, url, year_start):
        self.spark = SparkSession.builder.appName("Collector").getOrCreate()
        self.url = url
        self.year_start = year_start
        self.metadados_table = "metadata_infodengue"
        self.create_metadados_table()

    def create_metadados_table(self):
        if not self.spark._jsparkSession.catalog().tableExists(self.metadados_table):
            schema = """
                cod_municipio STRING,
                disease STRING,
                ano_ultima_coleta INT,
                semana_ultima_coleta INT,
                data_ultima_coleta TIMESTAMP
            """
            self.spark.sql(f"CREATE TABLE {self.metadados_table} ({schema}) USING DELTA")

    def get_municipios(self):
        query = "SELECT cod_municipio FROM codigos_ibge_municipios"
        df = self.spark.sql(query)
        return [row.cod_municipio for row in df.collect()]
    
    def save_json(self, data, disease, cod_municipio, year):
        file_path = f"/dbfs/mnt/datalake/info-dengue/raw/{disease}/{cod_municipio}-{year}.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

    def update_metadados(self, cod_municipio, disease, year, week):
        self.spark.sql(f"""
            MERGE INTO {self.metadados_table} USING (SELECT '{cod_municipio}' as cod_municipio, '{disease}' as disease) AS new_data
            ON {self.metadados_table}.cod_municipio = new_data.cod_municipio AND {self.metadados_table}.disease = new_data.disease
            WHEN MATCHED THEN
                UPDATE SET ano_ultima_coleta = {year}, semana_ultima_coleta = {week}, data_ultima_coleta = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (cod_municipio, disease, ano_ultima_coleta, semana_ultima_coleta, data_ultima_coleta) VALUES (new_data.cod_municipio, new_data.disease, {year}, {week}, current_timestamp())
        """)

    def get_last_collected_week(self, cod_municipio, disease):
        result = self.spark.sql(f"""
            SELECT ano_ultima_coleta, semana_ultima_coleta FROM {self.metadados_table}
            WHERE cod_municipio = '{cod_municipio}' AND disease = '{disease}'
        """).collect()
        if result:
            return result[0].semana_ultima_coleta, result[0].ano_ultima_coleta
        else:
            return 1, self.year_start

    def get_and_save(self, disease, cod_municipio, year_start, year_end):
        last_collected_week, last_collected_year = self.get_last_collected_week(cod_municipio, disease)
        current_year = datetime.now().year
        current_week = self.get_current_week()
        
        for year in range(max(year_start, last_collected_year), year_end + 1):
            if year == last_collected_year and current_week <= last_collected_week and year == current_year:
                print(f"Os dados de {disease} do município {cod_municipio} para o ano {year} já estão atualizados até a semana {last_collected_week}.")
                continue
            
            ew_start = last_collected_week + 1 if year == last_collected_year else 1
            ew_end = current_week if year == current_year else 53
            
            params = {
                "geocode": cod_municipio,
                "disease": disease,
                "format": "json",
                "ew_start": str(ew_start),
                "ew_end": str(ew_end),
                "ey_start": str(year),
                "ey_end": str(year),
            }
            response = requests.get(self.url, params=params)
            if response.status_code == 200:
                try:
                    data = response.json()
                    self.save_json(data, disease, cod_municipio, year)
                    self.update_metadados(cod_municipio, disease, year, min(int(ew_end), current_week))
                    print(f"Coleta concluída para {disease} em {cod_municipio} do ano {year}.")
                except json.JSONDecodeError:
                    print(f"Erro ao decodificar JSON: {disease}, {cod_municipio}, {year}")
            else:
                print(f"Request sem sucesso: {response.status_code}")
            time.sleep(5)

    def get_current_week(self):
        return datetime.now().isocalendar().week -1

    def collect_data(self, diseases, year_start, year_end):
        municipios = self.get_municipios()
        for disease in diseases:
            for cod_municipio in municipios:
                print(f"Iniciando coleta dos dados de {disease} do município {cod_municipio}...")
                self.get_and_save(disease, cod_municipio, year_start, year_end)

# COMMAND ----------

url = "https://info.dengue.mat.br/api/alertcity"
diseases = ["dengue", "zika", "chikungunya"]
year_start = 2024
year_end = datetime.now().year

collector = Collector(url, year_start)
collector.collect_data(diseases, year_start, year_end)
