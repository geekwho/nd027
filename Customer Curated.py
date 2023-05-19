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

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://geekwho/accelerometer/"], "recurse": True},
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node CustomerTrusted
CustomerTrusted_node1684356434843 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://geekwho/customers/trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1684356434843",
)

# Script generated for node Join Customer
JoinCustomer_node1684356307778 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1684356434843,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1684356307778",
)

# Script generated for node Drop Fields
DropFields_node1684356749419 = DropFields.apply(
    frame=JoinCustomer_node1684356307778,
    paths=["z", "y", "x", "timeStamp", "user"],
    transformation_ctx="DropFields_node1684356749419",
)

# Script generated for node CustomerCurated
CustomerCurated_node1684356611350 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1684356749419,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://geekwho/customers/curated/", "partitionKeys": []},
    transformation_ctx="CustomerCurated_node1684356611350",
)

job.commit()
