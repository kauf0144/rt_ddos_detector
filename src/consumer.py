import pyspark
import os
import sys
import time
from kafka import KafkaConsumer, TopicPartition
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *

# Need environment variables set to access local resources (for debug/testing):
java_home = "/usr/local/Cellar/openjdk@8/1.8.0+272"
spark_home = "/Users/bradleykaufman/Projects/Personal/PhData/resources/spark-3.0.1-bin-hadoop3.2"
os.environ['SPARK_HOME'] = spark_home
os.environ['JAVA_HOME'] = java_home

# Add the PySpark directories to the Python path:
sys.path.insert(1, os.path.join(spark_home, 'python'))
sys.path.insert(1, os.path.join(spark_home, 'python', 'pyspark'))
sys.path.insert(1, os.path.join(spark_home, 'python', 'build'))

# Need to configure Spark and Instantiate Spark Context:
conf = pyspark.SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '3'), \
                                   ('spark.cores.max', '3'), ('spark.driver.memory','8g')])
sc = pyspark.SparkContext(conf=conf)

# Using Spark Structured Streaming App (DDOS_DETECTOR) With Spark SQL
spark = SparkSession \
    .builder \
    .appName("DDOS_DETECTOR") \
    .master("spark://localhost:7077") \
    .getOrCreate()

# Need to establish Stream Source (KafkaConsumer)
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "http_logs") \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", False) \
  .load()

# Need to create the schema for incoming messages
logSchema = StructType([ \
  StructField('host', StringType()), \
  StructField('identity', StringType()), \
  StructField('user', StringType()), \
  StructField('time', StringType()), \
  StructField('request', StringType()), \
  StructField('status', StringType()), \
  StructField('bytes', StringType()), \
  StructField('referer', StringType()), \
  StructField('full_user_agent',  StringType())
  ])

# Initial Query to Deserialize
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json("value", schema=logSchema) \
    .alias("json")) \
    .select("json.*")

# View to be used in Spark SQL Aggregate Below
df.createOrReplaceTempView("logs")

# Want to figure out which user-agents are most frequent -- select all hosts frm the most frequent
df = df.groupBy("full_user_agent").agg(count(col("time")).alias("hits"),collect_set(col("host")).alias("hosts"))\
       .orderBy(col("hits"),ascending=False).limit(1).select("hosts")

# Write stream to Kafka topic "hosts"
df.selectExpr( "CAST(hosts AS STRING) as value") \
   .writeStream \
   .outputMode("complete") \
   .format("kafka") \
   .trigger(processingTime="2 minutes") \
   .option("kafka.bootstrap.servers", "localhost:9092") \
   .option("topic", "hosts") \
   .option("checkpointLocation","/tmp/checkpoints") \
   .start() \
   .awaitTermination(150)

# Write Kafka output from topic "hosts" to file (output.txt)
consumer = KafkaConsumer("hosts", bootstrap_servers='127.0.0.1:9092',
                         consumer_timeout_ms=60000)
file = open("../output.txt", "wb")
for message in consumer:
    file.write(message.value)
consumer.close()
file.close()
