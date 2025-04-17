# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# The imports and variables below are provided by the _common notebook above:
# dbutils - Databricks utilities
# DBAcademyHelper, course_config, lesson_config - Course setup utilities


# Mount the S3 bucket
mount_s3_bucket()

# COMMAND ----------

# Set up spark configuration for SQL access to data paths
setup_spark_conf()

# COMMAND ----------

# Reset working directory for lab exercises
reset_working_dir()

# COMMAND ----------

print("Classroom setup complete!")
print(f"Data paths:")
print(f"- Sales data: {sales_path}")
print(f"- Users data: {users_path}")
print(f"- Events data: {events_path}")
print(f"- Products data: {products_path}")
print(f"Working directory: {working_dir}")
print(f"Checkpoints directory: {checkpoints_dir}")
