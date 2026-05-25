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

# Script generated for node Amazon S3
AmazonS3_node1779686200700 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-himank/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1779686200700")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1779686291286 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1779686291286")

# Script generated for node SQL Query
SqlQuery734 = '''
select a.* from a join b on a.user=b.email

'''
SQLQuery_node1779686224439 = sparkSqlQuery(glueContext, query = SqlQuery734, mapping = {"a":AmazonS3_node1779686200700, "b":AWSGlueDataCatalog_node1779686291286}, transformation_ctx = "SQLQuery_node1779686224439")

# Script generated for node Amazon S3
AmazonS3_node1779686432817 = glueContext.getSink(path="s3://stedi-human-balance-analytics-himank/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779686432817")
AmazonS3_node1779686432817.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="accelerometer_trusted")
AmazonS3_node1779686432817.setFormat("json")
AmazonS3_node1779686432817.writeFrame(SQLQuery_node1779686224439)
job.commit()