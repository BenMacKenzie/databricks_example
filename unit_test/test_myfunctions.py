import pytest
import pyspark
from myfunctions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType



# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

def test_tableNotEmpty():
  df = read_table("ben_mackenzie", "tpch", "customer")
  assert(df.count() > 0)

