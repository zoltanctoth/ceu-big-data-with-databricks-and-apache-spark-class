# Databricks notebook source
# MAGIC %md # ASP 1.2 - S3

# COMMAND ----------

# MAGIC %md Set up the credentials:

# COMMAND ----------

AWS_ACCESS_KEY_ID='AKIA4VGB3BTUT756...'
AWS_SECRET_ACCESS_KEY='anQfAsezODDShZFj6j2g...'
your_bucket = "ceu-tothz"

# COMMAND ----------

# MAGIC %md Let's take a look at my bucket:

# COMMAND ----------

files = dbutils.fs.ls(f"s3a://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY.replace('/', '%2F')}@{your_bucket}")
display(files)

# COMMAND ----------

files = dbutils.fs.ls(f"s3a://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY.replace('/', '%2F')}@{your_bucket}/datalake/")
display(files)

# COMMAND ----------

# MAGIC %md Read a file from the bucket and display it:

# COMMAND ----------

df = spark.read.parquet(f"s3a://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY.replace('/', '%2F')}@{your_bucket}/datalake/silver_edits")
display(df)
