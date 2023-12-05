import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# imports
import json
import time
from json import dumps
import datetime
from kafka import KafkaProducer
import requests

#Sets
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,LongType,ArrayType,BooleanType,DateType
import pyspark.sql.functions  as sf
spark = SparkSession.builder \
    .appName("POC") \
    .master("local[2]") \
    .config("spark.driver.host", "localhost")\
    .getOrCreate()
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

#Functions
def dataframe_to_rdd(df):
    rdd = df.rdd.map(lambda row: row.asDict(True))
    def transform(row):
        row["timestamp_kafka"]= str(datetime.datetime.now())
        return row
    rdd = rdd.map(lambda row: transform(row))

    return rdd

def rdd_to_tuple(rdd):
    tup = tuple(rdd.collect())
    return tup

def get_last_process():
    df_get = spark.read.csv( ).withColumnRenamed('_c0', 'last_process')

    time_lp = df_get.select('last_process').collect()

    return time_lp[0].last_process


#variables
body = []
lenResponse = 1000
iterationCount = 0
URL = "url-hash"
# TOKEN = "token-hash"
TOKEN = "basic 64 token hash"
time_ini = 1683299700

# Starts iterations in API
while lenResponse == 1000 and iterationCount < 10:
    # Set variable
    split = 0

    # Parameters for Request
    querystring = {"start_time": time_ini}

    headers = {
        'content-type': "application/json",
        'authorization': TOKEN,
    }

    # Runs the Request in the API
    response = requests.request(
        "GET", URL, headers=headers, params=querystring
    )

    body_list = json.loads(response.content.decode('utf-8'))
    # Handles API response
    try:
        body_page = body_list['tickets']
    except:
        # For request limit wait 1 minute
        msg_error = body_list['error']
        if msg_error == 'APIRateLimitExceeded':
            time.sleep(60)
            split = 1
        else:
            raise Exception(body_list['description'])

    # Successful request goes on adding the data
    if split == 0:
        time_ini = body_list['end_time']
        print(time_ini)

        # Counts records in call
        lenResponse = len(body_page)
        iterationCount += 1

        # Union informations
        df = spark.sparkContext.parallelize(body_page).map(lambda x: json.dumps(x))
        df = spark.read.json(df)
        tup = rdd_to_tuple(dataframe_to_rdd(df))

        for row in tup:
            producer.send('kttm', json.dumps(row).encode('utf=8'))
            

# Dataframe create
# df = spark.sparkContext.parallelize(body).map(lambda x: json.dumps(x))
# df = spark.read.json(df)

# df2 = df.select(sf.col("name"),sf.to_json(sf.struct($"*"))).toDF("key", "value")
# df2.show(false)

# tup = rdd_to_tuple(dataframe_to_rdd(df))

# for row in tup:
#     producer.send('kttm', json.dumps(row).encode('utf=8'))
#     print ('Message sent ', row)

# df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").show()

# df.select(sf.to_json(sf.struct("*")).alias("value"))\
#   .selectExpr("CAST(value AS STRING)")\
#   .write\
#   .format("console")\
#   .option("kafka.bootstrap.servers", "localhost:9092")\
#   .option("topic", "kttm")\
#   .save()\

# df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")\
#    .write\
#    .format("kafka")\
#    .mode("append")\
#    .option("kafka.bootstrap.servers", "localhost:9092")\
#    .option("topic", "josn_data_topic")\
#    .save()



spark.stop()


