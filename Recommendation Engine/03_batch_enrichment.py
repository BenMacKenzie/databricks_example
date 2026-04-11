# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Batch Enrichment Pipeline
# MAGIC
# MAGIC This is the key pattern: instead of a real-time RAG agent, we **pre-assemble all context in batch**
# MAGIC using Vector Search + joins, then pass the full context to an LLM via `ai_query()`.
# MAGIC
# MAGIC ### Why batch over RAG?
# MAGIC - **Simpler**: no agent framework, no retrieval chain, no prompt orchestration
# MAGIC - **Cheaper**: batch inference pricing, no always-on serving endpoint
# MAGIC - **Auditable**: every input/output is a row in a Delta table
# MAGIC - **Scalable**: runs on Spark — handles 5K customers as easily as 5M

# COMMAND ----------

CATALOG = "serverless_stable_1dpktm_catalog"
SCHEMA = "recommendation_engine"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.courses_vs_index"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Build a search query per customer from their profile
# MAGIC
# MAGIC We create a natural-language search string from each customer's attributes
# MAGIC so Vector Search returns the most relevant courses for them.

# COMMAND ----------

from pyspark.sql import functions as F

customers = spark.table(f"{CATALOG}.{SCHEMA}.customers")

# Build a search query that captures what this customer would benefit from
customers_with_query = customers.withColumn(
    "search_query",
    F.concat_ws(
        " ",
        F.col("role"),
        F.col("industry"),
        F.col("experience"),
        F.col("interests"),
        F.col("designation"),
        F.lit("accounting courses")
    )
)

customers_with_query.createOrReplaceTempView("customers_with_query")
display(spark.sql("SELECT customer_id, name, search_query FROM customers_with_query LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Vector Search retrieval using `vector_search()` SQL function
# MAGIC
# MAGIC Instead of calling the SDK in a Python loop, we use the built-in `vector_search()` function
# MAGIC which runs natively in Spark SQL — no collect, no loop, fully parallelized.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW course_matches AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   vs.*
# MAGIC FROM customers_with_query c,
# MAGIC LATERAL vector_search(
# MAGIC   index => 'serverless_stable_1dpktm_catalog.recommendation_engine.courses_vs_index',
# MAGIC   query => c.search_query,
# MAGIC   num_results => 10
# MAGIC ) vs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick check: how many matches per customer?
# MAGIC SELECT customer_id, COUNT(*) as courses_matched
# MAGIC FROM course_matches
# MAGIC GROUP BY customer_id
# MAGIC ORDER BY courses_matched
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Aggregate matched courses into a single text block per customer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW courses_aggregated AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(*) as num_courses_retrieved,
# MAGIC   concat_ws('\n\n',
# MAGIC     collect_list(
# MAGIC       concat(
# MAGIC         '**', title, '** (ID: ', course_id, ')\n',
# MAGIC         '   Domain: ', domain, ' | Level: ', level, ' | Format: ', format, ' | Duration: ', duration, '\n',
# MAGIC         '   CE Credits: ', CAST(ce_credits AS STRING), ' | Price: $', CAST(price AS STRING), '\n',
# MAGIC         '   Description: ', description
# MAGIC       )
# MAGIC     )
# MAGIC   ) AS relevant_courses
# MAGIC FROM course_matches
# MAGIC GROUP BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Join customer profile + retrieved courses into enriched context table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE serverless_stable_1dpktm_catalog.recommendation_engine.enriched_customers AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.name,
# MAGIC   c.email,
# MAGIC   c.designation,
# MAGIC   c.role,
# MAGIC   c.industry,
# MAGIC   c.company_size,
# MAGIC   c.experience,
# MAGIC   c.province,
# MAGIC   c.interests,
# MAGIC   c.courses_completed,
# MAGIC   c.ce_credits_needed,
# MAGIC   c.preferred_format,
# MAGIC   c.budget_range,
# MAGIC   ca.relevant_courses,
# MAGIC   ca.num_courses_retrieved
# MAGIC FROM customers_with_query c
# MAGIC JOIN courses_aggregated ca ON c.customer_id = ca.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_enriched FROM serverless_stable_1dpktm_catalog.recommendation_engine.enriched_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we've built
# MAGIC
# MAGIC Each row in `enriched_customers` now has:
# MAGIC - Full customer profile (role, industry, interests, CE needs, budget)
# MAGIC - Top 10 relevant courses (retrieved via `vector_search()` SQL function)
# MAGIC
# MAGIC **Key difference from a RAG approach:** the entire retrieval step is a single SQL query — no Python SDK, no loops, no `collect()`. It runs natively in Spark, fully parallelized.
# MAGIC
# MAGIC **Next step**: pass each row to `ai_query()` to generate personalized recommendations and email copy.
