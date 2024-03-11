import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from awsglue.dynamicframe import DynamicFrame

logger = logging.getLogger("my_logger")
logger.setLevel(logging.INFO)

# create a handler for cloudwatch
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info("My log message")

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1710150595953 = glueContext.create_dynamic_frame.from_catalog(
    database="smartcity",
    table_name="emergency_data",
    transformation_ctx="AmazonS3_node1710150595953",
)

logger.info("print schema of node Amazon s3 for dynamic frame")
AmazonS3_node1710150595953.printSchema()

count = AmazonS3_node1710150595953.count()
print("Number of rows in dynamic frame: ", count)
logger.info("count for frame is {}".format(count))

# Script generated for node Change Schema
ChangeSchema_node1710150704681 = ApplyMapping.apply(
    frame=AmazonS3_node1710150595953,
    mappings=[
        ("id", "string", "id", "string"),
        ("vehicleid", "string", "new_vehicleid", "string"),
        ("timestamp", "timestamp", "new_timestamp", "timestamp"),
        ("location", "string", "new_location", "string"),
        ("incidentid", "string", "new_incidentid", "string"),
        ("incidenttype", "string", "new_incidenttype", "string"),
        ("status", "string", "new_status", "string"),
        ("description", "string", "new_description", "string"),
        ("ds", "string", "new_ds", "string"),
    ],
    transformation_ctx="ChangeSchema_node1710150704681",
)

# convert dynamic frame to spark dataframe
logger.info("convert dynamic frame to spark dataframe")

spark_dataframe = ChangeSchema_node1710150704681.toDF()

spark_dataframe.show()

logger.info("convert spark dataframe into a table view so that we can run SQL query on it")
spark_dataframe.createOrReplaceTempView("emergency_view")

logger.info("create dataframe by spark sql")
emergency_sql_df = spark.sql("SELECT new_incidenttype, count(id) AS incident_cnt FROM emergency_view GROUP BY new_incidenttype")

logger.info("Display results after aggregation")
emergency_sql_df.show()

# convert spark dataframe back to glue dynamic dataframe
logger.info("convert the dataframe back to dynamic dataframe")
dynamic_frame = DynamicFrame.fromDF(emergency_sql_df, glueContext, "dynamic_frame")

# Script generated for node Amazon S3
AmazonS3_node1710150708140 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://smartcity-etl/output/", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1710150708140",
)

job.commit()
