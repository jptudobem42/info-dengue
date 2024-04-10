# Databricks notebook source
#!pip install xlrd
import xlrd

# COMMAND ----------

import pandas as pd

filename = "/dbfs/mnt/datalake/info-dengue/datasets/RELATORIO_DTB_BRASIL_MUNICIPIO.xls"
df = pd.read_excel(filename, skiprows=6, header=0)

df = df[['UF', 'Nome_UF', 'Código Município Completo', 'Nome_Município']]

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# Cria sessão Spark

spark = (SparkSession.builder
                    .appName("CodigosMunicipios")
                    .config("spark.some.config.option", "some-value")
                    .getOrCreate()
        )

# COMMAND ----------

# Cria um DataFrame com o Spark

schema = StructType([
    StructField("UF", IntegerType(), True),
    StructField("Nome_UF", StringType(), True),
    StructField("Código Município Completo", IntegerType(), True),
    StructField("Nome_Município", StringType(), True)
])

df_spark = spark.createDataFrame(df, schema=schema)

# COMMAND ----------

# Renomea as colunas

df_municipios = df_spark.select(
    col("UF").alias("cod_uf"),
    col("Nome_UF").alias("uf"),
    col("Código Município Completo").alias("cod_municipio"),
    col("Nome_Município").alias("municipio")
)

df_municipios.display()

# COMMAND ----------

# Escreve o DataFrame Spark em uma tabela Delta

df_municipios.write.format("delta").mode("overwrite").saveAsTable("codigos_ibge_municipios")
