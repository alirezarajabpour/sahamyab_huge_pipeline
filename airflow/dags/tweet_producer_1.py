import six
import sys

# Workaround for six.moves import error in Python 3.12
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves


from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time
import requests
from json import dumps
from kafka import KafkaProducer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 18),
    'email': ['alizarajabpoor@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



def filter_data(item):
    if item.get("image"):
        del item["image"]
    cleansed_data = {
        "id": item['id'],
        "content": item['content'],
        "sendTime": item['sendTime'],
        "sendTimePersian": item['sendTimePersian'],
        "senderName": item['senderName'],
        "senderUsername": item['senderUsername'],
        "type": item['type'],
        "raw-data": item
    }
    return cleansed_data

def fetch_and_send_tweets():
    producer = KafkaProducer(bootstrap_servers=['kafka1:29092', 'kafka2:29093', 'kafka3:29094'], value_serializer=lambda x: dumps(x).encode('utf-8'),
                             key_serializer=str.encode,api_version=(0,11,5))
    TOPIC_NAME = "Sahamyab-Tweets"

    url = "https://www.sahamyab.com/guest/twiter/list?v=0.1"
    delay = 10
    cnt = 0
    response = requests.request('GET', url, headers={'User-Agent': 'Chrome/81'})
    if response.status_code == requests.codes.ok:
        items = response.json()['items']
        for item in items:
            cnt += 1
            print("-=-" * 20)
            print(f"#{cnt:4} - {item['id']}")

            if not item.get('content'):
                print("tweet is unacceptable")
                continue
            else:
                data = filter_data(item)
                producer.send(TOPIC_NAME, value=data, key=item['id'])
    else:
        print("Error in fetch data: {err}".format(err=response.status_code))
    time.sleep(delay)


with DAG(
    'sahamyab_tweets_1',
    default_args=default_args,
    description='A simple DAG to fetch tweets and send to Kafka',
    schedule_interval=timedelta(minutes=10),
    catchup=False
) as dag:
    fetch_tweets_task = PythonOperator(
    task_id='fetch_and_send_tweets',
    python_callable=fetch_and_send_tweets,
    dag=dag,
    )

fetch_tweets_task
