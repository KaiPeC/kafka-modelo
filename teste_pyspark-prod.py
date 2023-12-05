import json
import pandas as pd
from json import dumps
import datetime
from kafka import KafkaProducer
from time import sleep

toke = "3ArunJohLnObbzDbqdhTNT96n8WX4L4goyNEPvvm"


from pyspark.sql import SparkSession
import pyspark.sql.functions  as sf
spark = SparkSession.builder \
    .appName("POC") \
    .master("local[2]") \
    .config("spark.driver.host", "localhost")\
    .getOrCreate()

js = spark.read.json('/home/magalu/Documentos/teste.json')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
value_serializer=lambda x:dumps(x)

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

tup = rdd_to_tuple(dataframe_to_rdd(js))

for row in tup:
    producer.send('kttm', json.dumps(row).encode('utf=8'))
    sleep(2)
    print ('Message sent ', row)

spark.stop()


