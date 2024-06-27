

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import threading
import time
import requests
import os
from tqdm import tqdm
from dotenv import load_dotenv

import requests
from bs4 import BeautifulSoup
import time
import pymongo
from pymongo import InsertOne
from dotenv import load_dotenv
from datetime import datetime
import os
load_dotenv(override=True)

from seleniumbase import SB
from consume.utils import  Redis, Kafka

crawlbot_server = os.getenv('CRAWLBOT_SERVER')

class Mogi:
    def crawl_url(self,page,proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/mogi/crawl_url?page={page}')
        return url.json()

    def crawl_data_by_url(self,url,proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/mogi/crawl_data_by_url?url={url}')
        return url.json()


class Batdongsan:
    def crawl_url(self,page,proxy=None):
        if proxy is not None:
            url = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_url?page={page}&proxy={proxy}')
        else:
            url = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_url?page={page}')
        if url.status_code == 200:
            return list(url.json())
        else:
            print('Error when crawl url from batdongsan.com.vn, status code: ', url.status_code)
            return []

    def crawl_data_by_url(self,url,proxy=None):
        if proxy is not None:
            data = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_data_by_url?url={url}&proxy={proxy}')
        else:
            data = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_data_by_url?url={url}')
        if data.status_code == 200:
            return data.json()
        else:
            print('Error when crawl data by url from batdongsan.com.vn, status code: ', data.status_code)
            return None

class Muaban:
    def crawl_id(self,offset,proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/muaban/crawl_id?offset={offset}')
        return url.json()

    def crawl_data_by_id(self, id, proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/muaban/crawl_data_by_id?id={id}')
        return url.json()

class Meeyland:
    def crawl_data_by_page(self, page, proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/meeyland/crawl_id?page={page}')
        return url.json()


def crawl_batdongsan_by_url(url):
    data = Batdongsan().crawl_data_by_url(url)
    if data is not None:
        if Kafka().send_data(data,'raw_batdongsan') == True:
            Redis().add_id_to_set(url, 'raw_batdongsan')

    return data

def crawl_muaban_by_id(id):
    if Redis().check_id_exist(id, 'raw_muaban'):
        print("Existed Crawled Id")
        return
    data = Muaban().crawl_data_by_id(id)
    if data is not None:
        if Kafka().send_data(data,'raw_muaban') == True:
            Redis().add_id_to_set(id, 'raw_muaban')
    else:
        print('crawl fail : ', id)

connection_str = os.getenv('REALESTATE_DB')
__client = pymongo.MongoClient(connection_str)

database = 'realestate'
__database = __client[database]

collection = __database["realestate_url_pool"]


def crawl_meeyland_by_page(page):
    data = Meeyland().crawl_data_by_page(page)
    if data is not None:
        operations = []
        for item in data:
            if Redis().check_id_exist(item['_id'], 'raw_meeyland'):
                continue
            if Kafka(broker_id = 0).send_data(item,'raw_meeyland') == True:
                Redis().add_id_to_set(item['_id'], 'raw_meeyland')
                operations.append(
                    InsertOne({
                        "crawl_at": datetime.now(),
                        "url": item['_id'],
                        "source": "meeyland"
                    })
                )
                print("Insert 1 record ok")
        if len(operations):
            collection.bulk_write(operations,ordered=False)
        return data

    return []


def crawl_meeyland():
    for page in tqdm(range(50, 500)):
        data = crawl_meeyland_by_page(page)
        time.sleep(5)
crawl_meeyland()