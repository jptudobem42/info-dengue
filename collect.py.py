# Databricks notebook source
import pandas as pd
import requests 

# COMMAND ----------

class Collector:
    def __init__(self, url):
        self.url = url

    def get_endpoint(self, **kwargs):
        resp = requests.get(self.url, params=kwargs)
        return resp
    
    def get_data(self, **kwargs):
        resp = self.get_endpoint(**kwargs)
        if resp.status_code == 200:
            data = resp.json()
            return data
        else:
            return {}

# COMMAND ----------

url = 'https://info.dengue.mat.br/api/alertcity'

params = {    
    "disease": "dengue",
    "geocode": "3106200",
    "format": "json",
    "ew_start": "1",
    "ew_end": "50",
    "ey_start": "2024",
    "ey_end": "2024"
}

collector = Collector(url)
data = collector.get_data(**params)

df = pd.DataFrame(data)

print(data.head())
