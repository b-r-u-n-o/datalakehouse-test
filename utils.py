
import json
import time
import requests
import logging
import os

from delta.tables import DeltaTable
from confluent_kafka import Producer
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)

class ExtractDataApi:
    
    def __init__(self, url: str, request):
        
        self.req = request
        self.url = url        
    
    def get_data(self):
        
        try:
            response = self.req.get(self.url)
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except Exception as e:
            raise e
        
class KafkaProducer:
    
    def __init__(self, producer:Producer):
        
        self.producer = producer({'bootstrap.servers': 'kafka-broker:9092'})
        
    def send_data(self, topic:str, data:dict):
        
        try:
            data = json.dumps(data)
            self.producer.produce(topic, value=data.encode('utf-8'), callback=self.verify)
            self.producer.flush()
            
        except Exception as e:
            raise e
    
    def verify(self, err, msg):
        if err is not None:
            logging.error(f'Message delivery failed: {err}')
        else:
            logging.info(f'Message delivered to {msg.topic} [{msg.partition()}], {msg.offset()}')

class SparkStreaming:
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("MinIO Example") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9050") \
            .config("spark.hadoop.fs.s3a.access.key", "datalake") \
            .config("spark.hadoop.fs.s3a.secret.key", "datalake") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
    
    def write_data_streaming(self, df, output_path:str):
        query = df.writeStream \
            .format('json') \
            .outputMode('append') \
            .option('path', output_path) \
            .option('checkpointLocation', './checkpoint') \
            .trigger(processingTime='30 seconds') \
            .start()
        
        return query.awaitTermination()

# class SparkStreaming:
    
#     def __init__(self):

#         # self.spark = SparkSession.builder \
#         #     .appName('ContabilizeiTeste') \
#         #     .getOrCreate()
#         self.spark = SparkSession.builder \
#             .appName("MinIO Example") \
#             .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#             .config("spark.hadoop.fs.s3a.access.key", "datalake") \
#             .config("spark.hadoop.fs.s3a.secret.key", "datalake") \
#             .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#             .getOrCreate()
    #     self.sc = self.spark.sparkContext
    #     self._config_minio()
    
    # def _config_minio(self):
    #     try:
    #         self.sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "datalake")
    #         self.sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "datalake")
    #         self.sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    #         self.sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    #         self.sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    #         self.sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    #         self.sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        
        # except Exception as e:
        #     raise e
        
    def read_data_streaming(self, topic:str):
        
        df = self.spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'kafka-broker:9092') \
            .option('subscribe', topic) \
            .load()
        
        return df

    # def write_data_streaming(self, df, output_path:str):
            
    #     query = df.writeStream \
    #         .format('json') \
    #         .outputMode('append') \
    #         .option('path', output_path) \
    #         .option('checkpointLocation', 'checkpoint') \
    #         .trigger(processingTime='30 seconds') \
    #         .start()
        
    #     return query.awaitTermination()

class DeltaOperations(SparkStreaming):

    def __init__(self):

        self.spark = super().spark
        self.delta_table = DeltaTable.forPath(self.spark, 'path')        

    def merge_data(self, df):
            
        commit = (
            self.delta_table
                .alias('old') 
                .merge(
                    df.alias('new'), 
                    'old.id = new.id'
                ) 
                .whenMatchedUpdateAll(
                  set={
                    "first_name": "new.first_name",
                    "last_name": "new.last_name",
                    "email": "new.email",
                    "date_of_birth": "new.date_of_birth"
                }) 
                .whenNotMatchedInsertAll(
                    values={
                    "id": "new.id",
                    "first_name": "new.first_name",
                    "last_name": "new.last_name",
                    "email": "new.email",
                    "date_of_birth": "new.date_of_birth"
                }
            ) 
            .execute()
        )
        return commit            
        
        

def run():

    url = 'https://random-data-api.com/api/v2/users?size=1'
    r = requests

    start = time.time()
    while start < 600:
        e = ExtractDataApi(url, r)
        time.sleep(3)
        data = e.get_data()

        p = Producer
        queue = KafkaProducer(p)
        queue.send_data('topic', data)

        spark = SparkStreaming()
        topic = 'user'
        df = spark.read_data_streaming(topic)
        df.printSchema()

        output_path = 's3a://raw/users/'
        spark.write_data_streaming(df, output_path)
        # query = (
        #     df.writeStream 
        #       .queryName("my_streaming_table")  
        #       .outputMode("append") 
        #       .format("memory") 
        #       .start()
        # )

        # spark.sql("SELECT * FROM my_streaming_table").show()

        # query.stop()