import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

def read_table(catalog, schema, table):
  return spark.table(f"{catalog}.{schema}.{table}")
  