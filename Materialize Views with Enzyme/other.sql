-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### AES Encrypt demo

-- COMMAND ----------

SELECT base64(aes_encrypt('Spark', 'abcdefghijklmnop'));


-- COMMAND ----------

SELECT cast(aes_decrypt(unbase64('n+3vEkhWHYpP+caZZMYGpmYWNaIgGQzFEStFa7nWrV+Y'),
                          'abcdefghijklmnop') AS STRING);
