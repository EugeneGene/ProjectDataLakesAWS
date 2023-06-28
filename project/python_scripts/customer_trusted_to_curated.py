import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1687863924697 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1687863924697",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-project-3/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node1687864000866 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1687863924697,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyJoin_node1687864000866",
)

# Script generated for node Drop Fields
DropFields_node1687864486576 = DropFields.apply(
    frame=CustomerPrivacyJoin_node1687864000866,
    paths=["z", "y", "x", "timeStamp", "user"],
    transformation_ctx="DropFields_node1687864486576",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1687873205359 = DynamicFrame.fromDF(
    DropFields_node1687864486576.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1687873205359",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1687873205359,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-project-3/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()