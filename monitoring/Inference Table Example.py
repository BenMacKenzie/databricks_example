# Databricks notebook source
pip install Faker

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ben_mackenzie.monitor_demo2;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use ben_mackenzie.monitor_demo2

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, when, expr, col, lit, unix_timestamp, explode, array, struct, to_timestamp
from random import choices
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Define number of customers and days
num_customers = 100
num_days = 30

# Define options for tier and label columns
tier_options = ["enterprise", "standard"]
label_options = ["yes", "no"]
prediction_options = ["yes", "no"]

# Define customers range
customers = range(1, num_customers+1)

# Parameterize time range
start_date = '2023-01-01'

# DataFrame with customer_id and a range of dates for each customer
df = spark.range(1, num_customers+1).selectExpr("id as customer_id").withColumn("date_array", explode(array([lit(x) for x in pd.date_range(start=start_date, periods=num_days)])))

df = df.selectExpr("customer_id", "date(date_array) as timestamp")

# Add utilization columns - this will add a random number between 0 and 100 for each record
df = df.withColumn("job_dbu", rand() * 15000).withColumn("interactive_dbu", rand() * 10000).withColumn("sql_dbu", rand() * 5000)

df = df.withColumn("job_dbu", col("job_dbu").cast("int"))
df = df.withColumn("interactive_dbu", col("interactive_dbu").cast("int"))
df = df.withColumn("sql_dbu", col("sql_dbu").cast("int"))
df = df.withColumn('timestamp', to_timestamp(col('timestamp')))

# Add tier column - this will randomly assign 'enterprise' or 'standard' to each record
df = df.withColumn("tier", expr(f"case when rand() < 0.8 then '{tier_options[0]}' else '{tier_options[1]}' end"))

# Add label column - this will randomly assign 'yes' or 'no' to each record
df = df.withColumn("label", expr(f"case when rand() < 0.1 then '{label_options[0]}' else '{label_options[1]}' end"))

# Add model_version column - this will be always 1
df = df.withColumn("model_version", lit(1))

# Add prediction column - this will be same as label 95% of the time

df = df.withColumn("prediction",
    when(
        col("label") == "yes", 
        expr(f"case when rand() < 0.95 then 'yes' else 'no' end")
    ).otherwise(
        expr(f"case when rand() < 0.90 then 'no' else 'yes' end")
    )
)

df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('churn')


# COMMAND ----------

# MAGIC %md
# MAGIC save the baseline table

# COMMAND ----------

df.sample(.1).write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable('base_churn')

# COMMAND ----------

# MAGIC %md 
# MAGIC add some data with different distribution

# COMMAND ----------

num_customers = 100
num_days = 60

# Define options for tier and label columns
tier_options = ["enterprise", "standard"]
label_options = ["yes", "no"]
prediction_options = ["yes", "no"]

# Define customers range
customers = range(1, num_customers+1)

# Parameterize time range
start_date = '2023-02-01'


# DataFrame with customer_id and a range of dates for each customer
df = spark.range(1, num_customers+1).selectExpr("id as customer_id").withColumn("date_array", explode(array([lit(x) for x in pd.date_range(start=start_date, periods=num_days)])))

df = df.selectExpr("customer_id", "date(date_array) as timestamp")

# Add utilization columns - this will add a random number between 0 and 100 for each record
df = df.withColumn("job_dbu", rand() * 20000).withColumn("interactive_dbu", rand() * 10000).withColumn("sql_dbu", rand() * 5000)

df = df.withColumn("job_dbu", col("job_dbu").cast("int"))
df = df.withColumn("interactive_dbu", col("interactive_dbu").cast("int"))
df = df.withColumn("sql_dbu", col("sql_dbu").cast("int"))
df = df.withColumn('timestamp', to_timestamp(col('timestamp')))

# Add tier column - this will randomly assign 'enterprise' or 'standard' to each record
df = df.withColumn("tier", expr(f"case when rand() < 0.7 then '{tier_options[0]}' else '{tier_options[1]}' end"))

# Add label column - this will randomly assign 'yes' or 'no' to each record
df = df.withColumn("label", expr(f"case when rand() < 0.15 then '{label_options[0]}' else '{label_options[1]}' end"))

# Add model_version column - this will be always 1
df = df.withColumn("model_version", lit(1))

# Add prediction column - this will be same as label 95% of the time

df = df.withColumn("prediction",
    when(
        col("label") == "yes", 
        expr(f"case when rand() < 0.95 then 'yes' else 'no' end")
    ).otherwise(
        expr(f"case when rand() < 0.90 then 'no' else 'yes' end")
    )
)

df.write.mode('append').saveAsTable('churn')

# COMMAND ----------

# MAGIC %sql
# MAGIC update churn set interactive_dbu = interactive_dbu + 1000 where date(timestamp) > '2023-02-01' and tier = 'enterprise'

# COMMAND ----------

# MAGIC %sql
# MAGIC --put in some 0's
# MAGIC update churn set sql_dbu = 0 where customer_id in (2,6,99)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- put in some Nulls
# MAGIC update churn set interactive_dbu = Null where customer_id = 76

# COMMAND ----------

# MAGIC %md
# MAGIC 1. distrubution of job_dbu changes in Feb/March for all customer
# MAGIC 2. distribution of interactive changes for enterprise customers in feburary/march 
# MAGIC 3. distribution of prediction and labels changes for all customers in feb/march

# COMMAND ----------

# MAGIC %pip install "https://ml-team-public-read.s3.us-west-2.amazonaws.com/wheels/data-monitoring/a4050ef7-b183-47a1-a145-e614628e3146/databricks_data_monitoring-0.1.0-py3-none-any.whl"

# COMMAND ----------

import databricks.data_monitoring as dm
from databricks.data_monitoring import analysis

# COMMAND ----------


SLICING_EXPRS = ["tier = 'enterprise'"]  
dm.create_or_update_monitor(
table_name="ben_mackenzie.monitor_demo2.churn",
analysis_type=dm.analysis.InferenceLog(
problem_type="classification",
timestamp_col="timestamp",
prediction_col="prediction",
model_version_col="model_version",
label_col = 'label',
example_id_col = 'customer_id'
),
slicing_exprs=SLICING_EXPRS, 
granularities=["1 month"],
baseline_table_name="ben_mackenzie.monitor_demo2.base_churn",
linked_entities=["models:/renewal_model"],
)

# COMMAND ----------

#call this in a job with scheudule to re-calculate drift
dm.refresh_metrics(table_name='ben_mackenzie.monitor_demo.churn')

# COMMAND ----------

#dm.delete_monitor(table_name="ben_mackenzie.monitor_demo.dbu", purge_artifacts=True)

# COMMAND ----------


