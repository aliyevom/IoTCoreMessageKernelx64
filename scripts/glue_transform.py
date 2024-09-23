import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Arguments for Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 input and output paths
input_path = "s3://sdk-signalIoT/raw/"
output_path = "s3://sdk-signalIoT/transformed/"

# Read raw data from S3
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", 
    connection_options={"paths": [input_path]}, 
    format="json"
)

# Transform the data (example: converting to Parquet format)
transformed_df = dynamic_frame.toDF()
transformed_df.write.mode("overwrite").parquet(output_path)

# Commit the Glue job
job.commit()
