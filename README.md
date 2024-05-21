# Data streaming pipeline - Airflow, Kafka, Spark, Postgres

This repository contains code for data-streaming

## First, we run the DAG `sahamyab_tweets_1`, followed by `sahamyab_pipeline_2` in the Airflow web UI.
## Now, we have the final data on the `Sahamyab-Tweets2` topic in Kafka.
# ----------------------------------------------
# there are two Python codes in Spark apps directory :
## The first one is `to_csv.py` which takes data from the Kafka topic and saves them as a CSV file in `spark-apps/csv`.
``docker exec -it spark-master bash``
#
``cd /apps``
#
``spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 to_csv.py``
## Then, we create a table in Postgres as follows:
`docker exec -it postgres-server bash`
## “Access the `stocks` database using the `Test` username.”
`psql stocks Test`
## then
`CREATE TABLE tweets (
    id TEXT PRIMARY KEY,
    content TEXT,
    sendTime TIMESTAMP,
    sendTimePersian TEXT,
    senderName TEXT,
    senderUsername TEXT,
    type TEXT,
    hashtags TEXT
);`
## The next one is `insert_pgre.py` that inserts the data into the `tweets` table in Postgres.
`spark-submit --master spark://spark-master:7077 --jars postgresql-42.7.3.jar insert_pgre.py`
## To check the results in the table, use:
`select * from tweets;`
## There are additional applications in the ‘/apps/other-apps’ directory. You can run them by replacing ‘code-name’ with the name of your application in the following command:
`spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 code-name`
