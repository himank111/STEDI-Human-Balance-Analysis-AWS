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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1779684949060 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1779684949060")

# Script generated for node SQL Query
SqlQuery747 = '''
select * 
from myDataSource 
where sharewithresearchasofdate is not null

'''
SQLQuery_node1779684659943 = sparkSqlQuery(glueContext, query = SqlQuery747, mapping = {"myDataSource":AWSGlueDataCatalog_node1779684949060}, transformation_ctx = "SQLQuery_node1779684659943")

# Script generated for node Amazon S3
AmazonS3_node1779685015806 = glueContext.getSink(path="s3://stedi-human-balance-analytics-himank/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779685015806")
AmazonS3_node1779685015806.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_trusted")
AmazonS3_node1779685015806.setFormat("json")
AmazonS3_node1779685015806.writeFrame(SQLQuery_node1779684659943)
job.commit()