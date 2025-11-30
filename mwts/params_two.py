import json
import requests
import psycopg2
import psycopg2.extras
import datetime
import pandas as pd
import numpy as np
import time

DB_HOST = "db"
DB_PORT = 5432
DB_NAME = "airflow_db"
DB_USER = "airflow"
DB_PASSWORD = "airflow"

BATCH_SIZE = 10

API_CHANNELS = "https://tables.mws.ru/fusion/v1/datasheets/dstoLqB3Hci6MqrEKH/records?viewId=viwXBJe2lMPB4&fieldKey=name"
API_VIDEO = "https://tables.mws.ru/fusion/v1/datasheets/dstjZauwdMNFY6Hpna/records?viewId=viwPY9zDNXr8n&fieldKey=name" 
API_ANALYTICS = "https://tables.mws.ru/fusion/v1/datasheets/dstA1SyvbzU8ZfccCo/records?viewId=viw0tueWmVSiA&fieldKey=name"
API_TOKEN = "uskTQa9DcjynSKX1WZmi9iP"
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

def connect_to_db():
    print('Connecting to the Postgresql db')
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user = DB_USER,
            password = DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        print(e)
        raise
