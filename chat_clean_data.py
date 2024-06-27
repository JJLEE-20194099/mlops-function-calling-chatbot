import threading
import func_timeout
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
from utils.meeyland_util import transferMeeyland
import json
from tqdm import tqdm
from consume.utils import Redis
from dotenv import load_dotenv
import os
load_dotenv(override=True)

class Kafka:
    def __init__(self, broker_id):
        self.kafka_host = os.getenv('KAFKA_HOST')
        self.broker_id = broker_id
        self.kafka_port = os.getenv(f'KAFKA_PORT_{self.broker_id}')
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'])
        #self.consumer = KafkaConsumer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'], auto_offset_reset='earliest', enable_auto_commit=True, group_id=self.kafka_group_id,value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    def kafka_consumer(self, kafka_group_id, kafka_topic):
        """_summary_

        Args:
            kafka_group_id (_type_): group id of consumer
            kafka_topic (_type_): list topic to subscribe

        Returns:
            _type_: consumer
        """
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=kafka_group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            max_poll_records=10
        )
        consumer.subscribe(kafka_topic)
        return consumer

    def send_data(self, data,kafka_topic):
        """_summary_

        Args:
            data (_type_): data to send to kafka
            kafka_topic (_type_): topic to send data

        Returns:
            _type_: False if send fail, True if send success
        """
        status = self.producer.send(kafka_topic, value = json.dumps(data).encode('utf-8'))
        self.producer.flush()
        if status.is_done == True:
            return True
        else:
            return False


    def create_consumer_and_subscribe(self, kafka_group_id, kafka_topic):
        """_summary_

        Args:
            kafka_group_id (_type_): group id of consumer
            kafka_topic (_type_): list topic to subscribe

        Returns:
            _type_ : consumer
        """
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'], auto_offset_reset='earliest', enable_auto_commit=True, group_id=kafka_group_id,value_deserializer=lambda x: x.decode('utf-8'))
        consumer.subscribe(kafka_topic)
        return consumer


KafkaInstance = Kafka(broker_id = 0)
MAX_THREAD = 10

with open('streets.json', encoding='utf-8') as f:
   streets = json.load(f)


locationql = [f'{item["STREET"].lower()}, {item["WARD"].lower()}, {item["DISTRICT"].lower()}' for item in tqdm(streets)]

def processMeeyland(msg):
    data = msg.value
    dataMeeyland = transferMeeyland(data)
    if dataMeeyland != None:
        status = KafkaInstance.send_data(dataMeeyland, "datn_meeyland")
        if status:
            print("Process New Message and Send Message Done")
            return dataMeeyland
    return None


def clean_meeyland():
    consumer = KafkaInstance.kafka_consumer("raw_meeyland", ["raw_meeyland"])
    for msg in tqdm(consumer):

        if Redis().check_id_exist(f'meeyland_offset_{msg.offset}', 'meeyland_clean_rawdata'):
            print("Ignore Processed Messages")
            continue
        Redis().add_id_to_set(f'meeyland_offset_{msg.offset}', 'meeyland_clean_rawdata')
        processMeeyland(msg)


clean_meeyland()