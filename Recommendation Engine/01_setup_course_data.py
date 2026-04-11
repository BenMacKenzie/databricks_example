# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Setup Course Data & Vector Search Index
# MAGIC
# MAGIC Creates a catalog of 1,000 accounting/finance courses and builds a Vector Search index on course descriptions.

# COMMAND ----------

CATALOG = "serverless_stable_1dpktm_catalog"
SCHEMA = "recommendation_engine"
VECTOR_SEARCH_ENDPOINT = "recommendation_engine_vs"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate 1,000 Synthetic Courses

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import random
from faker import Faker
from pyspark.sql import Row

fake = Faker()
Faker.seed(42)
random.seed(42)

CATALOG = "serverless_stable_1dpktm_catalog"
SCHEMA = "recommendation_engine"
VECTOR_SEARCH_ENDPOINT = "recommendation_engine_vs"

# Course topic building blocks
domains = [
    "Tax", "Audit", "Financial Reporting", "Forensic Accounting", "Managerial Accounting",
    "Cost Accounting", "Government Accounting", "Non-Profit Accounting", "International Accounting",
    "Bookkeeping", "Payroll", "Accounts Payable", "Accounts Receivable", "Treasury",
    "Risk Management", "Internal Controls", "Compliance", "Ethics", "Sustainability Reporting",
    "Data Analytics", "ESG Reporting", "Cryptocurrency Accounting", "M&A Accounting",
    "Revenue Recognition", "Lease Accounting", "Consolidation", "Transfer Pricing",
    "Budgeting & Forecasting", "Cash Flow Management", "Financial Planning & Analysis"
]

levels = ["Fundamentals", "Intermediate", "Advanced", "Masterclass", "Certification Prep", "Workshop", "Bootcamp"]

specializations = [
    "for Small Business", "for Public Companies", "for Startups", "for Healthcare",
    "for Real Estate", "for Manufacturing", "for Retail", "for Technology Companies",
    "for Financial Services", "for Energy Sector", "for Canadian Professionals",
    "for CPA Candidates", "for CFOs", "for Controllers", "using AI & Automation",
    "using Excel & Power BI", "using Python", "with Case Studies", "for Cross-Border Operations",
    "for Private Equity"
]

formats = ["Self-Paced Online", "Live Virtual", "In-Person", "Hybrid", "On-Demand Video"]
durations = ["2 hours", "4 hours", "1 day", "2 days", "3 days", "1 week", "4 weeks", "8 weeks", "12 weeks"]
ce_credits = [0, 1, 2, 3, 4, 5, 8, 10, 15, 20, 24, 40]

courses = []
for i in range(1, 1001):
    domain = random.choice(domains)
    level = random.choice(levels)
    spec = random.choice(specializations)
    title = f"{domain} {level} {spec}"

    description = (
        f"This {level.lower()} course covers {domain.lower()} concepts {spec.lower()}. "
        f"Participants will learn {fake.sentence(nb_words=12).lower()} "
        f"Topics include {fake.sentence(nb_words=10).lower()} "
        f"The course emphasizes practical application through real-world scenarios and "
        f"hands-on exercises. Ideal for accounting professionals looking to deepen their "
        f"expertise in {domain.lower()} with a focus on {spec.lower().replace('for ', '').replace('using ', '')}. "
        f"{fake.sentence(nb_words=15)}"
    )

    courses.append(Row(
        course_id=f"COURSE-{i:04d}",
        title=title,
        domain=domain,
        level=level,
        specialization=spec.replace("for ", "").replace("using ", ""),
        description=description,
        format=random.choice(formats),
        duration=random.choice(durations),
        ce_credits=random.choice(ce_credits),
        price=round(random.uniform(49, 2999), 2),
        rating=round(random.uniform(3.5, 5.0), 1),
        num_reviews=random.randint(5, 500)
    ))

courses_df = spark.createDataFrame(courses)
courses_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.courses")

display(spark.table(f"{CATALOG}.{SCHEMA}.courses").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed (required for Vector Search sync)

# COMMAND ----------

spark.sql(f"""
  ALTER TABLE {CATALOG}.{SCHEMA}.courses
  SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Endpoint & Index

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType

w = WorkspaceClient()

# Create endpoint (no-op if it already exists)
try:
    w.vector_search_endpoints.create_endpoint(
        name=VECTOR_SEARCH_ENDPOINT,
        endpoint_type=EndpointType.STANDARD
    )
    print(f"Creating endpoint '{VECTOR_SEARCH_ENDPOINT}'... this may take a few minutes.")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Endpoint '{VECTOR_SEARCH_ENDPOINT}' already exists.")
    else:
        raise e

# COMMAND ----------

# Wait for endpoint to be ready
import time

while True:
    ep = w.vector_search_endpoints.get_endpoint(VECTOR_SEARCH_ENDPOINT)
    status = ep.endpoint_status.state.value
    print(f"Endpoint status: {status}")
    if status == "ONLINE":
        break
    time.sleep(30)

# COMMAND ----------

# Create delta sync index on the courses table
source_table = f"{CATALOG}.{SCHEMA}.courses"
index_name = f"{CATALOG}.{SCHEMA}.courses_vs_index"

from databricks.sdk.service.vectorsearch import VectorIndexType, DeltaSyncVectorIndexSpecRequest, PipelineType, EmbeddingSourceColumn

try:
    w.vector_search_indexes.create_index(
        name=index_name,
        endpoint_name=VECTOR_SEARCH_ENDPOINT,
        primary_key="course_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=source_table,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[
                EmbeddingSourceColumn(name="description", embedding_model_endpoint_name="databricks-bge-large-en")
            ]
        )
    )
    print(f"Creating index '{index_name}'... this may take a few minutes.")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Index '{index_name}' already exists.")
    else:
        raise e

# COMMAND ----------

# Wait for index to be ready
while True:
    idx = w.vector_search_indexes.get_index(index_name)
    status = idx.status.ready
    print(f"Index ready: {status}")
    if status:
        print("Vector Search index is ready!")
        break
    time.sleep(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Test — Vector Search Query

# COMMAND ----------

results = w.vector_search_indexes.query_index(
    index_name=index_name,
    columns=["course_id", "title", "description", "level", "domain"],
    query_text="advanced tax planning strategies for cross-border operations",
    num_results=5
)

for r in results.result.data_array:
    print(f"{r[0]} | {r[1]}")
