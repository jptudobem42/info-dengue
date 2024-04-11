# Databricks notebook source
from datetime import datetime
import time
import csv
import os
import pandas as pd

import requests
from requests.sessions import Session

from multiprocessing import Pool

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

class Collector:
    def __init__(self, url, year_start):
        # Inicializa a sessão Spark
        self.spark = SparkSession.builder.appName("Collector").getOrCreate()
        self.session = requests.Session()
        self.url = url
        self.year_start = year_start
        self.metadados_table = "metadata_infodengue"
        self.create_metadados_table()

    def create_metadados_table(self):
        # Cria a tabela de metadados se não existir
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
        # Consulta os códigos dos municípios
        query = "SELECT cod_municipio FROM codigos_ibge_municipios"
        df = self.spark.sql(query)
        return [row.cod_municipio for row in df.collect()]

    def save_csv(self, data, disease, cod_municipio, year):
        file_path = f"/dbfs/mnt/datalake/info-dengue/raw/{disease}/{cod_municipio}-{year}.csv"
        file_exists = os.path.isfile(file_path)
        
        with open(file_path, mode='a') as file:
            if not file_exists:
                file.write(data)
            else:
                data_lines = data.splitlines()
                file.write("\n".join(data_lines[1:])) 

    def update_metadados(self, cod_municipio, disease, year, week):
        # Atualiza a tabela de metadados
        self.spark.sql(f"""
            MERGE INTO {self.metadados_table} USING (SELECT '{cod_municipio}' as cod_municipio, '{disease}' as disease) AS new_data
            ON {self.metadados_table}.cod_municipio = new_data.cod_municipio AND {self.metadados_table}.disease = new_data.disease
            WHEN MATCHED THEN
                UPDATE SET ano_ultima_coleta = {year}, semana_ultima_coleta = {week}, data_ultima_coleta = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (cod_municipio, disease, ano_ultima_coleta, semana_ultima_coleta, data_ultima_coleta) VALUES (new_data.cod_municipio, new_data.disease, {year}, {week}, current_timestamp())
        """)

    def get_last_collected_week(self, cod_municipio, disease):
        # Retorna a última semana coletada do município e doença especificados
        result = self.spark.sql(f"""
            SELECT ano_ultima_coleta, semana_ultima_coleta FROM {self.metadados_table}
            WHERE cod_municipio = '{cod_municipio}' AND disease = '{disease}'
        """).collect()
        if result:
            return result[0].semana_ultima_coleta, result[0].ano_ultima_coleta
        else:
            return 0, self.year_start

    def get_current_week(self):
        # Retorna a semana atual
        return datetime.now().isocalendar().week - 1

    def get_and_save(self, disease, cod_municipio, year_start, year_end):
        # Processa e salva os dados coletados
        last_collected_week, last_collected_year = self.get_last_collected_week(cod_municipio, disease)
        current_year = datetime.now().year
        current_week = self.get_current_week()

        for year in range(max(year_start, last_collected_year), year_end + 1):
            ew_start = last_collected_week + 1 if year == last_collected_year and last_collected_week < current_week else 1
            ew_end = current_week if year == current_year else 53

            if year == current_year and last_collected_week >= current_week:
                print(f"Nenhum dado novo para coletar para {disease} no município {cod_municipio} em {year}.")
                continue

            params = {
                "geocode": cod_municipio,
                "disease": disease,
                "format": "csv",
                "ew_start": str(ew_start),
                "ew_end": str(ew_end),
                "ey_start": str(year),
                "ey_end": str(year),
            }
            
            try:
                response = self.session.get(self.url, params=params)
                if response.status_code == 200:
                    data = response.text
                    self.save_csv(data, disease, cod_municipio, year)  # Salva como CSV
                    self.update_metadados(cod_municipio, disease, year, ew_end)
                    print(f"Coleta concluída para {disease} no município {cod_municipio} em {year} até a semana {ew_end}.")
                else:
                    print(f"Request sem sucesso: {response.status_code}")
            except Exception as e:
                print(f"Erro ao acessar a API para {disease} no município {cod_municipio}: {e}")
            time.sleep(2)

    def collect_data(self, diseases, year_start, year_end):
        # Inicia a coleta de dados
        municipios = self.get_municipios()
        for disease in diseases:
            for cod_municipio in municipios:
                print(f"Iniciando coleta dos dados de {disease} do município {cod_municipio}...")
                self.get_and_save(disease, cod_municipio, year_start, year_end)

# COMMAND ----------

url = "https://info.dengue.mat.br/api/alertcity"
diseases = ["dengue", "zika", "chikungunya"]
year_start = datetime.now().year -4
year_end = datetime.now().year

collector = Collector(url, year_start)
collector.collect_data(diseases, year_start, year_end)
