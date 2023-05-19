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
    connection_options={"paths": ["s3://geekwho/step_trainer/"], "recurse": True},
    transformation_ctx="StepTrainer_node1",
)

# Script generated for node CustomersCurated
CustomersCurated_node1684412245807 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://geekwho/customers/curated/"], "recurse": True},
    transformation_ctx="CustomersCurated_node1684412245807",
)

# Script generated for node Join
Join_node1684495187544 = Join.apply(
    frame1=StepTrainer_node1,
    frame2=CustomersCurated_node1684412245807,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1684495187544",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1684495187544,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://geekwho/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
