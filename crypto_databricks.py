# Databricks notebook source
# MAGIC %md
# MAGIC First add keys to databricks secrets with key vault backing.  see https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes
# MAGIC
# MAGIC In this example I haved added to keys 'class1' and 'class2'

# COMMAND ----------

# MAGIC %md
# MAGIC first a simple test

# COMMAND ----------

# MAGIC %sql
# MAGIC select base64(aes_encrypt('Spark', secret('encrypt', 'class1')))

# COMMAND ----------

# MAGIC %md 
# MAGIC And decrypt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(aes_decrypt(unbase64('49sh8T7iOdtMlACrmke90X9H3AQekYwp7/C7XO10koAW'), secret('encrypt', 'class1')) AS STRING);

# COMMAND ----------

# MAGIC %md
# MAGIC Now generate some data

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema crypto_test

# COMMAND ----------

# MAGIC %sql
# MAGIC create table crypto_test.customers(Name string, Address string, ssn string)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into crypto_test.customers values ('John Doe', 'halloween street, pasadena', '3567812');
# MAGIC insert into crypto_test.customers values ('Santa Claus', 'Christmas street, North Pole', '3568123');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe crypto_test.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC create view crypo_test_secure_customer_view as 
# MAGIC select Name, base64(aes_encrypt(Address, secret('encrypt', 'class1'))) as address_aes_1, base64(aes_encrypt(ssn, secret('encrypt', 'class2'))) as ssn_aes_2 from crypto_test.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from crypo_test_secure_customer_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, cast(aes_decrypt(unbase64(address_aes_1), secret('encrypt', 'class1')) AS STRING) as address, cast(aes_decrypt(unbase64(ssn_aes_2), secret('encrypt', 'class2')) AS STRING) as ssn from crypo_test_secure_customer_view

# COMMAND ----------

k = dbutils.secrets.get('encrypt', 'class1')

# COMMAND ----------

# MAGIC %sql
# MAGIC select base64(aes_encrypt('Spark', secret('encrypt', 'class1')))
