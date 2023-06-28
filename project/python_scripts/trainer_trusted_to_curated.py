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

# Script generated for node Trainer Landing
TrainerLanding_node1687917098902 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-project-3/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="TrainerLanding_node1687917098902",
)

# Script generated for node Customer Curated
CustomerCurated_node1687890610129 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1687890610129",
)

# Script generated for node Renamed keys for Customer Privacy Join
RenamedkeysforCustomerPrivacyJoin_node1687916664690 = ApplyMapping.apply(
    frame=CustomerCurated_node1687890610129,
    mappings=[
        ("customername", "string", "right_customername", "string"),
        ("email", "string", "right_email", "string"),
        ("phone", "string", "right_phone", "string"),
        ("birthday", "string", "right_birthday", "string"),
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("registrationdate", "long", "right_registrationdate", "long"),
        ("lastupdatedate", "long", "right_lastupdatedate", "long"),
        (
            "sharewithresearchasofdate",
            "string",
            "right_sharewithresearchasofdate",
            "string",
        ),
        (
            "sharewithpublicasofdate",
            "string",
            "right_sharewithpublicasofdate",
            "string",
        ),
        (
            "sharewithfriendsasofdate",
            "string",
            "right_sharewithfriendsasofdate",
            "string",
        ),
    ],
    transformation_ctx="RenamedkeysforCustomerPrivacyJoin_node1687916664690",
)

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node1687864000866 = Join.apply(
    frame1=RenamedkeysforCustomerPrivacyJoin_node1687916664690,
    frame2=TrainerLanding_node1687917098902,
    keys1=["right_serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="CustomerPrivacyJoin_node1687864000866",
)

# Script generated for node Drop Fields
DropFields_node1687864486576 = DropFields.apply(
    frame=CustomerPrivacyJoin_node1687864000866,
    paths=[
        "right_email",
        "right_phone",
        "right_customername",
        "right_birthday",
        "right_serialnumber",
        "right_registrationdate",
        "right_lastupdatedate",
        "right_sharewithresearchasofdate",
        "right_sharewithpublicasofdate",
        "right_sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1687864486576",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1687915351410 = DynamicFrame.fromDF(
    DropFields_node1687864486576.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1687915351410",
)

# Script generated for node Trainer Curated
TrainerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1687915351410,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-project-3/step_trainer/trainer_curated5/",
        "partitionKeys": [],
    },
    transformation_ctx="TrainerCurated_node3",
)

job.commit()
