# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# Mount the S3 bucket
mount_s3_bucket()

# COMMAND ----------


@DBAcademyHelper.monkey_patch
def create_table(self, table_name, location):
    import time

    start = int(time.time())

    print(f'Creating the table "{table_name}"', end="...")
    spark.sql(f"CREATE OR REPLACE TABLE {table_name} SHALLOW CLONE delta.`{location}`")

    print(f"({int(time.time())-start} seconds)")


# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

# Define paths directly instead of using DA.paths
events_path = "/mnt/data/v03/ecommerce/events/events.delta"
sales_path = "/mnt/data/v03/ecommerce/sales/sales.delta"
users_path = "/mnt/data/v03/ecommerce/users/users.delta"
products_path = "/mnt/data/v03/products/products.delta"

# Using data from mounted S3 bucket
DA.create_table("events", events_path)
DA.create_table("sales", sales_path)
DA.create_table("users", users_path)
DA.create_table("products", products_path)

DA.conclude_setup()
