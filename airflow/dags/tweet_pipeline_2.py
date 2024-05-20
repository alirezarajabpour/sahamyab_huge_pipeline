import six
import sys

# Workaround for six.moves import error in Python 3.12
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

import re
from json import dumps,loads
from kafka import KafkaConsumer, KafkaProducer
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


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


def get_tags(text):
    tags = re.findall(r"#(\w+)", text)
    return tags

def consum():
    consumer = KafkaConsumer(
        "Sahamyab-Tweets",
        bootstrap_servers=['kafka1:29092', 'kafka2:29093', 'kafka3:29094'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='hashtag-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        key_deserializer=lambda x : str(x)
    )
    return consumer


def send_tweets():
    producer = KafkaProducer(bootstrap_servers=['kafka1:29092', 'kafka2:29093', 'kafka3:29094'], value_serializer=lambda x: dumps(x).encode('utf-8'),
                            key_serializer=str.encode )
    
    consumer = consum()

    cnt=1
    for message in consumer:
        try : 
            data=message.value
            del data['raw-data']
            print("-=-"*20)
            print(f"#{cnt:4} - Processing Tweet ID {message.key}")
    #         print(f"Partition:{message.partition}\nOffset:{message.offset}\nKey:{message.key}\nValue:{pformat(message.value)}")
    #         print("-%-"*20)
            tags=get_tags(data['content'])
            print(f"Tags : {tags}")
            data["hashtags"] = tags
            producer.send("Sahamyab-Tweets2", value=data, key= data['id'])
            cnt+=1
        except Exception as ex:
            print("%%%-"*20)
            print(ex)



with DAG(
    'sahamyab_pipeline_2',
    default_args=default_args,
    description='A simple DAG to fetch tweets and send to Kafka',
    schedule_interval=timedelta(minutes=10),
    catchup=False
) as dag:
    send_tweets_task = PythonOperator(
        task_id='send_tweets_task',
        python_callable=send_tweets,
        dag=dag,
    )

# Set the task dependencies
send_tweets_task