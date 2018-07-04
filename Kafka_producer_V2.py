import os 
import pandas as pd 
import csv
import time
from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka import KafkaProducer
import json 
import requests


producer = KafkaProducer(bootstrap_servers='127.0.0.1:29092')

df = pd.read_csv("synop-data-meteo-cleaned.csv",delimiter=";")

df['lat'], df['lon'] = df['Coordonnees'].str.split(', ', 1).str
df["Température"] = df['Température']/10
df["Point de rosée"] = df["Point de rosée"]/10
df["Pression au niveau mer"] = df["Pression au niveau mer"]/100
df = df.rename(index=str, columns={"ID OMM station":"id","Date":"date","Pression au niveau mer":"pression","Température": "temperature", "Point de rosée": "rose","Humidité":"humidite","Coordonnees":"coord","Nom":"nom"})
#del df["Coordonnees"]

for i in range(df.shape[0]):
    line = df.iloc[i]
    out = line.to_json(orient='index')
    print(out)
    producer.send("raw_station_data", value=out.encode('utf-8'), key="mykey".encode('utf-8'))
    time.sleep(0.5)
