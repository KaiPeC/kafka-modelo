import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
# /home/magalu/Downloads/teste

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,LongType,ArrayType,BooleanType,DateType
from pyspark.sql.functions import col, from_json, lit, current_timestamp, sum

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1',
]

def save_to_mysql(df, batch_id):
    url = "jdbc:mysql://localhost:3306/TESTEKAFKA"

    df.write.jdbc(url=url+
                  "?user=<user>&password=<password>",
              table="tickets",
              mode="append",
              properties={"driver": 'com.mysql.cj.jdbc.Driver'})

    # df.withColumn("batchId", lit(batch_id)) \
    #   .write.jdbc(url= url) \
    #   .option("dbtable", "kafkaConsumer") \
    #   .option("user", "root") \
    #   .option("password", "root") \
    #   .mode("append") \
    #   .save()

def main():
    spark = SparkSession.builder \
        .appName("POC") \
        .master("local[2]") \
        .config("spark.sql.streaming.checkpointLocation", "checkpoint") \
        .config("spark.driver.host", "localhost")\
        .config("spark.driver.extraClassPath", "/home/User/Downloads/teste/mysql-connector-j-8.2.0.jar")\
        .getOrCreate()
#sua_tabela
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("auto.offset.reset", "latest") \
        .option("subscribe", "kttm") \
        .load()

    schema = StructType([
    StructField("url", StringType(), True),
    StructField("id", LongType(), True),
    StructField("external_id", StringType(), True),
    StructField("via_channel", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("type", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("raw_subject", StringType(), True),
    StructField("description", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("status", StringType(), True),
    StructField("recipient", StringType(), True),
    StructField("requester_id", LongType(), True),
    StructField("submitter_id", LongType(), True),
    StructField("assignee_id", LongType(), True),
    StructField("organization_id", LongType(), True),
    StructField("group_id", LongType(), True),
    StructField("forum_topic_id", StringType(), True),
    StructField("problem_id", LongType(), True),
    StructField("has_incidents", BooleanType(), True),
    StructField("is_public", BooleanType(), True),
    StructField("due_at", TimestampType(), True),
    StructField("ticket_form_id", LongType(), True),
    StructField("brand_id", LongType(), True),
    StructField("satisfaction_probability", StringType(), True),
    StructField("allow_channelback", BooleanType(), True),
    StructField("allow_attachments", BooleanType(), True),
    StructField("generated_timestamp", LongType(), True),
    StructField("datalog", DateType(), True),
    StructField("timestamp_kafka", TimestampType(), True)
    ]   )

    flatten_kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("tmp")) \
        .select("tmp.*")

    # aggregation = flatten_kafka_stream.groupBy(col("DeptName"),col("EmpName"),col("EmpId")).agg(sum(col("Salary")).alias("Salary"))
    # aggregation = aggregation.select(col("DeptName"),col("Salary"))
    kafka_to_mysql = flatten_kafka_stream \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(save_to_mysql) \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    main()
