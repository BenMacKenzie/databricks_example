# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Setup Customer Segmentation Data
# MAGIC
# MAGIC Creates a table of synthetic accountant customers with segmentation attributes for personalized course recommendations.

# COMMAND ----------

CATALOG = "serverless_stable_1dpktm_catalog"
SCHEMA = "recommendation_engine"

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import random
from faker import Faker
from pyspark.sql import Row

fake = Faker("en_CA")
Faker.seed(99)
random.seed(99)

CATALOG = "serverless_stable_1dpktm_catalog"
SCHEMA = "recommendation_engine"

# Segmentation dimensions
designations = ["CPA", "CPA,CA", "CPA,CMA", "CPA,CGA", "CPA Candidate", "Bookkeeper", "MBA,CPA"]
roles = [
    "Staff Accountant", "Senior Accountant", "Accounting Manager", "Controller",
    "CFO", "VP Finance", "Tax Manager", "Audit Manager", "Financial Analyst",
    "AP/AR Specialist", "Payroll Manager", "Forensic Accountant", "Consultant",
    "Partner", "Sole Practitioner"
]
industries = [
    "Public Accounting", "Technology", "Healthcare", "Manufacturing", "Retail",
    "Financial Services", "Real Estate", "Energy", "Government", "Non-Profit",
    "Professional Services", "Construction", "Agriculture", "Education", "Hospitality"
]
company_sizes = ["Solo", "Small (2-50)", "Medium (51-500)", "Large (501-5000)", "Enterprise (5000+)"]
experience_brackets = ["0-2 years", "3-5 years", "6-10 years", "11-15 years", "16-20 years", "20+ years"]
interest_pool = [
    "Tax Planning", "Audit & Assurance", "ESG Reporting", "Data Analytics",
    "AI & Automation", "International Standards", "Forensic Accounting",
    "M&A Advisory", "Risk Management", "Financial Modelling", "Budgeting",
    "Compliance & Regulation", "Sustainability", "Cryptocurrency",
    "Leadership & Management", "Excel & Tools", "Cloud Accounting",
    "Revenue Recognition", "Lease Accounting", "Transfer Pricing",
    "Non-Profit Accounting", "Government Accounting", "Payroll Compliance"
]

customers = []
for i in range(1, 5001):
    interests = random.sample(interest_pool, k=random.randint(2, 5))
    exp = random.choice(experience_brackets)
    role = random.choice(roles)

    # Bias courses_completed and ce_needed by experience
    exp_years = int("".join(c for c in exp.split("-")[0] if c.isdigit()))
    courses_completed = random.randint(0, min(exp_years * 3, 50))
    ce_needed = max(0, random.randint(0, 40) - courses_completed)

    customers.append(Row(
        customer_id=f"CUST-{i:05d}",
        name=fake.name(),
        email=fake.email(),
        designation=random.choice(designations),
        role=role,
        industry=random.choice(industries),
        company_size=random.choice(company_sizes),
        experience=exp,
        province=random.choice(["ON", "QC", "BC", "AB", "MB", "SK", "NS", "NB", "NL", "PE"]),
        interests=", ".join(interests),
        courses_completed=courses_completed,
        ce_credits_needed=ce_needed,
        preferred_format=random.choice(["Self-Paced Online", "Live Virtual", "In-Person", "Any"]),
        budget_range=random.choice(["Under $500", "$500-$1500", "$1500-$3000", "No Limit"])
    ))

customers_df = spark.createDataFrame(customers)
customers_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customers")

display(spark.table(f"{CATALOG}.{SCHEMA}.customers").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Stats

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as total_customers,
# MAGIC   COUNT(DISTINCT designation) as designations,
# MAGIC   COUNT(DISTINCT role) as roles,
# MAGIC   COUNT(DISTINCT industry) as industries
# MAGIC FROM serverless_stable_1dpktm_catalog.recommendation_engine.customers
