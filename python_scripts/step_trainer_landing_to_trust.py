import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://spark-s3-bucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node CustomerCurated
CustomerCurated_node1694992253013 = glueContext.create_dynamic_frame.from_catalog(
    database="glue-database",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1694992253013",
)

# Script generated for node Join
Join_node1694992275531 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerCurated_node1694992253013,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1694992275531",
)

# Script generated for node Amazon S3
AmazonS3_node1694992323734 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1694992275531,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://spark-s3-bucket/step_trainer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1694992323734",
)

job.commit()
