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

# Script generated for node StepTrainer
StepTrainer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://spark-s3-bucket/step_trainer/landing/step_trainer-1655296678763.json"
        ],
        "recurse": True,
    },
    transformation_ctx="StepTrainer_node1",
)

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1695012596819 = glueContext.create_dynamic_frame.from_catalog(
    database="glue-database",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1695012596819",
)

# Script generated for node Amazon S3
AmazonS3_node1695012669769 = glueContext.create_dynamic_frame.from_catalog(
    database="glue-database",
    table_name="customer_curated",
    transformation_ctx="AmazonS3_node1695012669769",
)

# Script generated for node Join
Join_node1695012635687 = Join.apply(
    frame1=StepTrainer_node1,
    frame2=AccelerometerLanding_node1695012596819,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1695012635687",
)

# Script generated for node Join
Join_node1695012686611 = Join.apply(
    frame1=AmazonS3_node1695012669769,
    frame2=Join_node1695012635687,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1695012686611",
)

# Script generated for node Amazon S3
AmazonS3_node1695012728435 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1695012686611,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://spark-s3-bucket/step_trainer/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1695012728435",
)

job.commit()
