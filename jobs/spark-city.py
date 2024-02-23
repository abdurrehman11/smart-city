from pyspark.sql import SparkSession, DataFrame
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col


def main():
    spark = (SparkSession.builder.appName("SmartCityStreaming")
            .config("spark.jar.packages", 
                     "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0",
                     "org.apache.hadoop:hadoop-aws:3.3.1",
                      "com.amazonaws:aws-java-sdk-core:1.11.469")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate())
    
    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel("WARN")

    # Vehicle schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    # GPS schema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    # Traffic schema
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("snapshot", StringType(), True)
    ])

    # Weather schema
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True)
    ])

    # Emergency schema
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("incidentType", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

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
    trafficDF = read_kafka_topic('traffic_camera_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_incident_data', emergencySchema).alias('emergency')

    query1 = streamWriter(input=vehicleDF, checkpointFolder="aws-s3-checkpoint-path", output="aws-s3-path")
    query2 = streamWriter(input=gpsDF, checkpointFolder="aws-s3-checkpoint-path", output="aws-s3-path")
    query3 = streamWriter(input=trafficDF, checkpointFolder="aws-s3-checkpoint-path", output="aws-s3-path")
    query4 = streamWriter(input=weatherDF, checkpointFolder="aws-s3-checkpoint-path", output="aws-s3-path")
    query5 = streamWriter(input=emergencyDF, checkpointFolder="aws-s3-checkpoint-path", output="aws-s3-path")

    query5.awaitTermination()


if __name__ == "__main__":
    main()
