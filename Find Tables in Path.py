# Databricks notebook source
# MAGIC %md 
# MAGIC One Database at a time using Regex

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract
file_prefix = "dbfs:/user/hive/warehouse/ben_churn_model.db/cust"
database = "ben_churn_model"
df = spark.sql(f"SHOW TABLE EXTENDED IN {database} LIKE '*'")
df = df.withColumn("location", regexp_extract(col('information'),'Location:\s(\S*\s)',1)).drop('isTemporary','information')
df = df.filter(df.location.startswith(file_prefix))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC All Databases as once.  No Regex

# COMMAND ----------

database_prefix = "\'ben_churn*\'"  #note the * and \`
file_prefix = "dbfs:/user/hive/warehouse/ben_churn_model.db/cust"
tbls_df = None
target_tables = []
for db in spark.sql(f"show databases like {database_prefix}").collect():
  #create a dataframe with list of tables from the database
  _df = spark.sql(f"show tables in {db.databaseName}")
  #union the tables list dataframe with main dataframe 
  if tbls_df is None:
    tbls_df = _df
  else:
    tbls_df = tbls_df.union(_df)
  
tables = tbls_df.collect()
for row in tables:
  databaseName = row['database']
  tableName = row['tableName']
  t_df = spark.sql(f"describe table extended {databaseName}.{tableName}")
  t_df = t_df.filter(t_df.col_name=='Location')
  table = t_df.filter(t_df.data_type.startswith(file_prefix)).collect()
  if table:
    target_tables.append((f"{databaseName}.{tableName}", table[0]['data_type']))

target_tables


# COMMAND ----------

# MAGIC %sql 
# MAGIC show databases like '*'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended ben_churn_model.dbu

# COMMAND ----------

target_tables

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(target_tables, columns =['databse', 'location'])
df.to_csv('test.csv')

# COMMAND ----------

df

# COMMAND ----------

df.toS
