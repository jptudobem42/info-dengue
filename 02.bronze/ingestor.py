# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, LongType

from delta import *
from datetime import datetime
import os

# COMMAND ----------

class BronzeIngestor:
    def __init__(self, spark, base_path, metadados_table):
        self.spark = spark
        self.base_path = base_path
        self.metadados_table = metadados_table

    def get_schema(self):
        # Define o schema dos dados a serem lidos dos arquivos CSV
        schema = StructType([
                StructField("data_ini_SE", DateType(), True),
                StructField("SE", IntegerType(), True),
                StructField("casos_est", FloatType(), True),
                StructField("cases_est_min", IntegerType(), True),
                StructField("cases_est_max", IntegerType(), True),
                StructField("casos", IntegerType(), True),
                StructField("p_rt1", FloatType(), True),
                StructField("p_inc100k", FloatType(), True),
                StructField("Localidade_id", IntegerType(), True),
                StructField("nivel", IntegerType(), True),
                StructField("id", LongType(), True),
                StructField("versao_modelo", DateType(), True),
                StructField("tweet", FloatType(), True),
                StructField("Rt", FloatType(), True),
                StructField("pop", FloatType(), True),
                StructField("tempmin", FloatType(), True),
                StructField("umidmax", FloatType(), True),
                StructField("receptivo", IntegerType(), True),
                StructField("transmissao", IntegerType(), True),
                StructField("nivel_inc", IntegerType(), True),
                StructField("umidmed", FloatType(), True),
                StructField("umidmin", FloatType(), True),
                StructField("tempmed", FloatType(), True),
                StructField("tempmax", FloatType(), True),
                StructField("casprov", IntegerType(), True),
                StructField("casprov_est", IntegerType(), True),
                StructField("casprov_est_min", IntegerType(), True),
                StructField("casprov_est_max", IntegerType(), True),
                StructField("casconf", IntegerType(), True),
                StructField("notif_accum_year", IntegerType(), True)
            ])
        return schema
    
    def get_data(self, disease):
        # Identifica os arquivos a serem lidos com base nos metadados para a doença específica
        current_year = datetime.now().year
        current_week = datetime.now().isocalendar().week -1
        
        print(f"Identificando arquivos para {disease} que precisam de atualização...")

        df_metadados = self.spark.sql(f"""
            SELECT cod_municipio, ano_ultima_coleta, semana_ultima_coleta
            FROM {self.metadados_table}
            WHERE disease = '{disease}' AND
                (ano_ultima_coleta < {current_year} OR 
                (ano_ultima_coleta = {current_year} AND semana_ultima_coleta = {current_week}))
        """)

        arquivos_para_ler = [
            f"{self.base_path}/{disease}/{row.cod_municipio}-{row.ano_ultima_coleta}.csv"
            for row in df_metadados.collect()
            if len(dbutils.fs.ls(f"{base_path}/{disease}/{row.cod_municipio}-{row.ano_ultima_coleta}.csv")) > 0
        ]
        
        if arquivos_para_ler:
            df = self.spark.read.csv(arquivos_para_ler, schema=self.get_schema(), header=True)
        else:
            print(f"Não foram encontrados arquivos atualizados para {disease}.")
            df = self.spark.createDataFrame([], self.get_schema())
        return df

    def filter_updates(self, df, disease):
        # Filtra as atualizações necessárias baseando-se nos metadados
        current_year = datetime.now().year
        current_week = datetime.now().isocalendar().week -1

        print(f"Filtrando dados que precisam ser atualizados para {disease}...")
        
        df.createOrReplaceTempView("updates")
        df_updates = self.spark.sql(f"""
                        SELECT u.*
                        FROM updates u
                        LEFT JOIN {self.metadados_table} m
                        ON LEFT(u.id, 7) = m.cod_municipio AND m.disease = '{disease}'
                        WHERE (m.ano_ultima_coleta IS NULL AND NOT EXISTS (
                                SELECT 1 FROM {self.metadados_table} m2
                                WHERE m2.cod_municipio = LEFT(u.id, 7) AND m2.disease = '{disease}'
                            )) OR
                            (m.ano_ultima_coleta < {current_year}) OR
                            (m.ano_ultima_coleta = {current_year} AND m.semana_ultima_coleta = {current_week})
                    """)
        return df_updates

    def verify_and_merge(self, disease, df_updates):
        # Verifica se a tabela existe e faz o merge dos dados
        disease_table = f"bronze.infodengue.{disease}"
        table_exist = self.spark.catalog.tableExists(disease_table)

        if not table_exist:
            print(f"Criando tabela Delta para {disease}.")
            df_updates.write.format("delta").mode("overwrite").saveAsTable(disease_table)
        else:
            print(f"Atualizando dados na tabela {disease_table}.")
            deltaTable = DeltaTable.forName(self.spark, disease_table)
            deltaTable.alias("main").merge(
                df_updates.alias("updates"),
                "main.id = updates.id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def run(self, diseases):
        # Executa o processo de ingestão para cada doença
        for disease in diseases:
            df_raw = self.get_data(disease)
            df_updates = self.filter_updates(df_raw, disease)
            if df_updates.count() > 0:
                self.verify_and_merge(disease, df_updates)
            else:
                print(f"Nenhum dado de {disease} para ser atualizado!")
            self.spark.sql(f"VACUUM {f'bronze.infodengue.{disease}'} RETAIN 240 HOURS")

# COMMAND ----------

spark = SparkSession.builder.appName("BronzeIngestor").getOrCreate()
base_path = "/mnt/datalake/info-dengue/raw"
metadados_table = "bronze.infodengue.metadata_infodengue"
diseases = ["dengue", "zika", "chikungunya"]

data_ingestor = BronzeIngestor(spark, base_path, metadados_table)
data_ingestor.run(diseases)
