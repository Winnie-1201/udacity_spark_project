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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="glue-database",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node CustomerTrusted
CustomerTrusted_node1694983301573 = glueContext.create_dynamic_frame.from_catalog(
    database="glue-database",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1694983301573",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1694983415396 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1694983301573,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1694983415396",
)

# Script generated for node Drop Fields
DropFields_node1694983547435 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1694983415396,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1694983547435",
)

# Script generated for node Amazon S3
AmazonS3_node1694983463842 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1694983547435,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://spark-s3-bucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1694983463842",
)

job.commit()
