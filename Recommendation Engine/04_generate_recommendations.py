# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Generate Personalized Recommendations with `ai_query()`
# MAGIC
# MAGIC This is where the magic happens. We take the enriched context table (customer profile + relevant courses)
# MAGIC and pass each row to an LLM using `ai_query()` — **no agent, no RAG chain, just SQL**.

# COMMAND ----------

CATALOG = "serverless_stable_1dpktm_catalog"
SCHEMA = "recommendation_engine"
MODEL = "databricks-gpt-oss-120b"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Build the prompt column
# MAGIC
# MAGIC We assemble a detailed prompt for each customer that includes all the context the LLM needs
# MAGIC to make personalized recommendations.

# COMMAND ----------

from pyspark.sql import functions as F

enriched = spark.table(f"{CATALOG}.{SCHEMA}.enriched_customers")

# Build the full prompt per customer
prompted = enriched.withColumn(
    "prompt",
    F.concat(
        F.lit("""You are a course recommendation specialist for a professional accounting education company.

Based on the customer profile and available courses below, recommend exactly 3 courses and write a personalized email.

## Customer Profile
- Name: """), F.col("name"),
        F.lit("\n- Designation: "), F.col("designation"),
        F.lit("\n- Role: "), F.col("role"),
        F.lit("\n- Industry: "), F.col("industry"),
        F.lit("\n- Company Size: "), F.col("company_size"),
        F.lit("\n- Experience: "), F.col("experience"),
        F.lit("\n- Province: "), F.col("province"),
        F.lit("\n- Interests: "), F.col("interests"),
        F.lit("\n- Courses Already Completed: "), F.col("courses_completed").cast("string"),
        F.lit("\n- CE Credits Needed: "), F.col("ce_credits_needed").cast("string"),
        F.lit("\n- Preferred Format: "), F.col("preferred_format"),
        F.lit("\n- Budget: "), F.col("budget_range"),
        F.lit("""

## Available Courses (ranked by relevance)
"""), F.col("relevant_courses"),
        F.lit("""

## Instructions
1. Select exactly 3 courses from the list above that best match this customer's profile, interests, and CE credit needs.
2. For each course, write 1-2 sentences explaining why it's a great fit for THIS specific customer.
3. Write a short, personalized email (under 200 words) recommending these 3 courses. Use a warm, professional tone. Include course IDs.
4. Output valid JSON only, in this exact format:

{
  "recommended_courses": [
    {"course_id": "COURSE-XXXX", "title": "...", "reason": "..."},
    {"course_id": "COURSE-XXXX", "title": "...", "reason": "..."},
    {"course_id": "COURSE-XXXX", "title": "...", "reason": "..."}
  ],
  "email_subject": "...",
  "email_body": "..."
}""")
    )
)

display(prompted.select("customer_id", "name", "prompt").limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Call `ai_query()` in batch
# MAGIC
# MAGIC This is the core of the pattern: one SQL function call processes every row.
# MAGIC No agent. No retrieval chain. Just a well-structured prompt with pre-assembled context.

# COMMAND ----------

prompted.createOrReplaceTempView("prompted_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE serverless_stable_1dpktm_catalog.recommendation_engine.recommendations AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   email,
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-120b',
# MAGIC     prompt
# MAGIC   ) AS llm_response
# MAGIC FROM prompted_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Parse the JSON responses

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE serverless_stable_1dpktm_catalog.recommendation_engine.parsed_recommendations AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   email,
# MAGIC   llm_response,
# MAGIC   from_json(llm_response, 'STRUCT<recommended_courses: ARRAY<STRUCT<course_id: STRING, title: STRING, reason: STRING>>, email_subject: STRING, email_body: STRING>') AS parsed
# MAGIC FROM serverless_stable_1dpktm_catalog.recommendation_engine.recommendations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   email,
# MAGIC   parsed.email_subject,
# MAGIC   parsed.email_body,
# MAGIC   parsed.recommended_courses
# MAGIC FROM serverless_stable_1dpktm_catalog.recommendation_engine.parsed_recommendations
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Quick Quality Check

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many responses parsed successfully?
# MAGIC SELECT
# MAGIC   COUNT(*) AS total,
# MAGIC   COUNT(parsed.email_subject) AS parsed_ok,
# MAGIC   COUNT(*) - COUNT(parsed.email_subject) AS parse_failures
# MAGIC FROM serverless_stable_1dpktm_catalog.recommendation_engine.parsed_recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC What we just did:
# MAGIC 1. **Assembled full context** per customer (profile + vector-search-retrieved courses)
# MAGIC 2. **Called `ai_query()` in batch** — the LLM got everything it needed in a single prompt
# MAGIC 3. **Parsed structured JSON output** into a clean table ready for the email system
# MAGIC
# MAGIC ### Why this beats a RAG agent for this use case
# MAGIC | | Batch `ai_query()` | RAG Agent |
# MAGIC |---|---|---|
# MAGIC | **Complexity** | SQL + one prompt template | Agent framework + retriever + chain + serving endpoint |
# MAGIC | **Cost** | Batch pricing, no idle compute | Always-on serving endpoint |
# MAGIC | **Auditability** | Every input & output in Delta | Requires custom logging |
# MAGIC | **Scale** | Spark-native, handles millions | Limited by endpoint throughput |
# MAGIC | **Latency** | Minutes for full run | Real-time per request (overkill here) |
