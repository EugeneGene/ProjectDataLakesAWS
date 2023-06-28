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

# Script generated for node Customer Trusted
CustomerTrusted_node1687863924697 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1687863924697",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1687922653197 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-project-3/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1687922653197",
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
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1687864486576",
)

# Script generated for node Accelerometer and Step Trainer Join
AccelerometerandStepTrainerJoin_node1687922733115 = Join.apply(
    frame1=DropFields_node1687864486576,
    frame2=StepTrainerLanding_node1687922653197,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="AccelerometerandStepTrainerJoin_node1687922733115",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=AccelerometerandStepTrainerJoin_node1687922733115,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-project-3/machine_learning/curated_timeStamp/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
