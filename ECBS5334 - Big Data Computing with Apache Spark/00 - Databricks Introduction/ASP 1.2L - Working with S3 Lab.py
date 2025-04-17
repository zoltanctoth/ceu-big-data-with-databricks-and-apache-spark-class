# Databricks notebook source
# MAGIC %md # Working with S3 - Lab
# MAGIC
# MAGIC Go to the S3 console and create an Access Key and Secret Key. Add them to the following cell:

# COMMAND ----------

AWS_ACCESS_KEY_ID='<<FILL-IN>>'
AWS_SECRET_ACCESS_KEY='<<FILL-IN>>'
your_bucket = "<<FILL-IN>>"

# COMMAND ----------

# MAGIC %md List the files in your bucket:

# COMMAND ----------

files = dbutils.fs.ls(f"s3a://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY.replace('/', '%2F')}@{your_bucket}/")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  1) Download the <a href="https://ceu-spark-data.s3.us-east-2.amazonaws.com/hosts.parquet" target="_blank">hosts.parquet</a> file to your computer and then upload it into your own S3 bucket using the AWS website (Or use Python to upload the file)
# MAGIC  2) In the next cell, read the parquet file from your own bucket and display it

# COMMAND ----------

# df = spark.<<FILL_IN>>
# display(df)
