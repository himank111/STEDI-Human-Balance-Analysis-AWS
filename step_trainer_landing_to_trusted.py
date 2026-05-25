import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Curated
CustomerCurated_node1779688321760 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-himank/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1779688321760")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1779688322322 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-himank/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1779688322322")

# Script generated for node Privacy Filter
SqlQuery757 = '''
select a.* from a join b on a.serialnumber=b.serialnumber
'''
PrivacyFilter_node1779688562417 = sparkSqlQuery(glueContext, query = SqlQuery757, mapping = {"a":StepTrainerLanding_node1779688322322, "b":CustomerCurated_node1779688321760}, transformation_ctx = "PrivacyFilter_node1779688562417")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1779688632642 = glueContext.getSink(path="s3://stedi-human-balance-analytics-himank/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1779688632642")
StepTrainerTrusted_node1779688632642.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1779688632642.setFormat("json")
StepTrainerTrusted_node1779688632642.writeFrame(PrivacyFilter_node1779688562417)
job.commit()