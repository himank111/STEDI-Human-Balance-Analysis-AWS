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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1779689246369 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1779689246369")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1779689246803 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1779689246803")

# Script generated for node SQL Query
SqlQuery711 = '''
select * from a join b on a.sensorreadingtime=b.timestamp
'''
SQLQuery_node1779689340515 = sparkSqlQuery(glueContext, query = SqlQuery711, mapping = {"a":StepTrainerTrusted_node1779689246369, "b":AccelerometerTrusted_node1779689246803}, transformation_ctx = "SQLQuery_node1779689340515")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1779689446120 = glueContext.getSink(path="s3://stedi-human-balance-analytics-himank/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1779689446120")
MachineLearningCurated_node1779689446120.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1779689446120.setFormat("json")
MachineLearningCurated_node1779689446120.writeFrame(SQLQuery_node1779689340515)
job.commit()