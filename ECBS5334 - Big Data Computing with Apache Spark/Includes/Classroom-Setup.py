# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# The imports and variables below are provided by the _common notebook above:
# dbutils - Databricks utilities
# DBAcademyHelper, course_config, lesson_config - Course setup utilities


# Mount the S3 bucket
mount_s3_bucket()

# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

# Remove paths from DA object and define them directly
sales_path = "/mnt/data/v03/ecommerce/sales/sales.delta"
users_path = "/mnt/data/v03/ecommerce/users/users.delta"
events_path = "/mnt/data/v03/ecommerce/events/events.delta"
products_path = "/mnt/data/v03/products/products.delta"

# Set these as spark configuration parameters so they can be accessed as ${var_name} in SQL
spark.conf.set("sales_path", sales_path)
spark.conf.set("users_path", users_path)
spark.conf.set("events_path", events_path)
spark.conf.set("products_path", products_path)

DA.conclude_setup()
