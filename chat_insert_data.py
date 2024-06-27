from kafka import KafkaConsumer
import pymongo
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import os
import math

from schema.preprocess.fillna import nan_2_none
load_dotenv(override=True)
from consume.utils import  Redis

from pymongo import InsertOne
from tqdm import tqdm


broker_id = 0
port = os.getenv(f"KAFKA_PORT_{broker_id}")
host = os.getenv("KAFKA_HOST")
kafka_broker = f'{host}:{port}'
kafka_topic = ['datn_meeyland']
kafka_group_id = 'datn_meeyland'
connection_str = os.getenv('REALESTATE_DB')
__client = pymongo.MongoClient(connection_str)

database = 'realestate'
__database = __client[database]

def consume_messages():


    consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=kafka_group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            max_poll_records=10
        )
    consumer.subscribe(kafka_topic)

    update_data_list = []
    updated_ids = []
    operations = []

    collection = __database["realestate_listing"]
    for message in tqdm(consumer):
        if Redis().check_id_exist(f'meeyland_offset_{message.offset}', 'meeyland_insert_db'):
            continue

        message_data = message.value

        Redis().add_id_to_set(f'meeyland_offset_{message.offset}', 'meeyland_insert_db')
        record = nan_2_none(message_data)
        operations.append(
            InsertOne(record)
        )

        if len(operations) >= 20:
            collection.bulk_write(operations,ordered=False)
            print(f"Insert batch size - {len(operations)} clean realestates to database")
            operations = []


    # print(collection')
    if len(operations):
        collection.bulk_write(operations,ordered=False)

    # => Trigger training AI Model

consume_messages()


