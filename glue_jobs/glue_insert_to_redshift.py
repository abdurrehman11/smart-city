import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1710184085393 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://smartcity-etl/output/"], "recurse": True},
    transformation_ctx="AmazonS3_node1710184085393",
)

# Script generated for node Change Schema
ChangeSchema_node1710184088535 = ApplyMapping.apply(
    frame=AmazonS3_node1710184085393,
    mappings=[
        ("new_incidenttype", "string", "new_incidenttype", "string"),
        ("incident_cnt", "long", "incident_cnt", "BIGINT"),
    ],
    transformation_ctx="ChangeSchema_node1710184088535",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1710184097340 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1710184088535,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://smartcity-etl/temp/",
        "useConnectionProperties": "true",
        "dbtable": "public.emergency_incident_stats",
        "connectionName": "MyRedshiftConnection",
        "preactions": "CREATE TABLE IF NOT EXISTS public.emergency_incident_stats (new_incidenttype VARCHAR, incident_cnt BIGINT);",
    },
    transformation_ctx="AmazonRedshift_node1710184097340",
)

job.commit()
