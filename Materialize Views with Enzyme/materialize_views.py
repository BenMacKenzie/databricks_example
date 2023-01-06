# Databricks notebook source
import dlt

def materialize_view(view_name):
  v = view_name.split(".")[1] #we don't want the schema
  table_name = f"{v}_m"
  @dlt.table(name=table_name)
  def mv():          
    return (
      spark.table(view_name)
    )
  



# COMMAND ----------

view_list = ['mv_demo.customer_view_1', 'mv_demo.customer_view_2']
for v in view_list:
  materialize_view(v)

# COMMAND ----------


