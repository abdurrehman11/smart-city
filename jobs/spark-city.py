from pyspark.sql import SparkSession, DataFrame
from config import configuration
from pyspark.sql.functions import from_json, col
from datetime import datetime
from schema import vehicleSchema, gpsSchema, trafficSchema, weatherSchema, emergencySchema
from dotenv import load_dotenv
import os

load_dotenv()

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming")\
            .config("spark.jars.packages", 
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.apache.hadoop:hadoop-aws:3.3.1,"
                      "com.amazonaws:aws-java-sdk-core:1.11.469")\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))\
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY"))\
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
            .getOrCreate()
    
    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel("WARN")

    # get ds for partition
    ds = datetime.now().strftime("%Y%m%d")

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes'))
    
    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())

    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    query1 = streamWriter(input=vehicleDF, 
                          checkpointFolder="s3a://smartcity-spark-streaming-data/checkpoints/vehicle_data", 
                          output=f"s3a://smartcity-spark-streaming-data/data/vehicle_data/ds={ds}")
    
    query2 = streamWriter(input=gpsDF, 
                          checkpointFolder="s3a://smartcity-spark-streaming-data/checkpoints/gps_data", 
                          output=f"s3a://smartcity-spark-streaming-data/data/gps_data/ds={ds}")
    
    query3 = streamWriter(input=trafficDF, 
                          checkpointFolder="s3a://smartcity-spark-streaming-data/checkpoints/traffic_data", 
                          output=f"s3a://smartcity-spark-streaming-data/data/traffic_data/ds={ds}")
    
    query4 = streamWriter(input=weatherDF, 
                          checkpointFolder="s3a://smartcity-spark-streaming-data/checkpoints/weather_data", 
                          output=f"s3a://smartcity-spark-streaming-data/data/weather_data/ds={ds}")
    
    query5 = streamWriter(input=emergencyDF, 
                          checkpointFolder="s3a://smartcity-spark-streaming-data/checkpoints/emergency_data", 
                          output=f"s3a://smartcity-spark-streaming-data/data/emergency_data/ds={ds}")

    query5.awaitTermination()


if __name__ == "__main__":
    main()
