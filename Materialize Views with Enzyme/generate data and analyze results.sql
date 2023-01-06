-- Databricks notebook source
-- MAGIC %md
-- MAGIC First create a table and two views.  The views are nearly identical but are used to illustrated dynamically creating tables using DLT.

-- COMMAND ----------

create database if not exists mv_demo;
CREATE OR REPLACE TABLE mv_demo.customers (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  last_name STRING,
  first_name STRING,
  phone STRING,
  SSN STRING,
  email STRING
);

-- COMMAND ----------

CREATE or replace VIEW mv_demo.customer_view_1 AS (
  SELECT 
  id ,
  last_name,
  first_name,
  phone,
  sha1(ssn) as sha1_ssn,
  sha1(email) as sha1_email,
  case when is_member('auditors') then email
  else '*****'
  end as email
  FROM  mv_demo.customers)

-- COMMAND ----------

CREATE or replace VIEW mv_demo.customer_view_2 AS (
  SELECT 
  id ,
  last_name,
  first_name,
  phone,
  sha1(ssn) as sha1_ssn,
  sha1(email) as sha1_email,
  case when is_member('auditors') then ssn
  else '*****'
  end as ssn
  FROM  mv_demo.customers)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC install Faker to generate customer-like data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pip install Faker

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from faker import Faker
-- MAGIC import pandas as pd
-- MAGIC 
-- MAGIC def add_new_customers(num):
-- MAGIC   fake=Faker()
-- MAGIC   data = [[fake.last_name(), fake.first_name(), fake.phone_number(), fake.ssn(), fake.ascii_company_email()] for i in range(num)]
-- MAGIC   df = pd.DataFrame(data, columns=['last_name','first_name', 'phone', 'ssn', 'email'])
-- MAGIC   spark.createDataFrame(df).write.format('delta').mode('append').saveAsTable('mv_demo.customers')
-- MAGIC 
-- MAGIC 
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md
-- MAGIC start by adding 1000 customers to base table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC add_new_customers(1000)

-- COMMAND ----------

select * from mv_demo.customer_view_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC set up a DLT workflow using materialize_views notebook: 
-- MAGIC 1. in the advanced configuration set pipelines.enzyme.mode to 'advanced'
-- MAGIC 2. select the 'preview' channel.
-- MAGIC 3. run the pipeline. 
-- MAGIC 4. look at materielized view

-- COMMAND ----------

select * from mv_demo.customer_view_1_m 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC insert more data 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC add_new_customers(1000)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. trigger DLT pipeline again
-- MAGIC 2. look at table history to confirm that new data was appended

-- COMMAND ----------

describe history mv_demo.customer_view_1_m 

-- COMMAND ----------


