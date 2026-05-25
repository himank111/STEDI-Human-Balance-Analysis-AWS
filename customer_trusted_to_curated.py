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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1779686933569 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1779686933569")

# Script generated for node Customer Trusted
CustomerTrusted_node1779686822726 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1779686822726")

# Script generated for node SQL Query
SqlQuery718 = '''
select distinct a.* from a join b on a.email=b.user

'''
SQLQuery_node1779686962851 = sparkSqlQuery(glueContext, query = SqlQuery718, mapping = {"a":CustomerTrusted_node1779686822726, "b":AccelerometerTrusted_node1779686933569}, transformation_ctx = "SQLQuery_node1779686962851")

# Script generated for node Customer Curated
CustomerCurated_node1779687058473 = glueContext.getSink(path="s3://stedi-human-balance-analytics-himank/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1779687058473")
CustomerCurated_node1779687058473.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_curated")
CustomerCurated_node1779687058473.setFormat("json")
CustomerCurated_node1779687058473.writeFrame(SQLQuery_node1779686962851)
job.commit()