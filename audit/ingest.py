# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists ben_mackenzie.audit

# COMMAND ----------

# MAGIC %sql
# MAGIC create table IF NOT EXISTS ben_mackenzie.audit.group_membership (group string, user string, userReference string, date date)

# COMMAND ----------

import requests
import json
import pandas as pd
from datetime import date


databricks_domain = "adb-984752964297111.11.azuredatabricks.net"
pat = dbutils.secrets.get(scope="bmac", key="pat")

headers = {
    "Authorization": f"Bearer {pat}",
    "Content-Type": "application/json"
}


res=requests.get(f"https://{databricks_domain}/api/2.0/account/scim/v2/Groups", headers=headers)
if res.status_code != 200:
    raise Exception(f'Bad status code. Expected: 200. Got: {res.status_code}')

resJson2=res.json()

rows = []
for resource in resJson2["Resources"]:
    group_name = resource["displayName"]
    if 'members' in resource:
            for member in resource["members"]:
                rows.append((group_name, member["display"], member["$ref"]))


# Create the Pandas DataFrame
df = pd.DataFrame(rows, columns=["group", "user", "userReference"])
df["date"] = date.today()

# Display the DataFrame
df = spark.createDataFrame(df)

df.write.mode("append").format("delta").saveAsTable("ben_mackenzie.audit.group_membership")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ben_mackenzie.audit.group_membership

# COMMAND ----------



# COMMAND ----------


