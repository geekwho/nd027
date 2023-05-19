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

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://geekwho/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node StepTrainer
StepTrainer_node1684356434843 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://geekwho/step_trainer/"], "recurse": True},
    transformation_ctx="StepTrainer_node1684356434843",
)

# Script generated for node Join Customer
JoinCustomer_node1684356307778 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainer_node1684356434843,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="JoinCustomer_node1684356307778",
)

# Script generated for node MachineLearningCurated
MachineLearningCurated_node1684356611350 = glueContext.write_dynamic_frame.from_options(
    frame=JoinCustomer_node1684356307778,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://geekwho/machine-learning-curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1684356611350",
)

job.commit()
