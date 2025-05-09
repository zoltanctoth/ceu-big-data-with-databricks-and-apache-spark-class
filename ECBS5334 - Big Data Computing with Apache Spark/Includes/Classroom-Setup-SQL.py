# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ceu;
# MAGIC USE ceu;

# COMMAND ----------

try:
    spark.sql("DROP VIEW IF EXISTS sales;")
except:
    spark.sql("DROP TABLE IF EXISTS sales;")

try:
    spark.sql("DROP VIEW IF EXISTS users;")
except:
    spark.sql("DROP TABLE IF EXISTS users;")

try:
    spark.sql("DROP VIEW IF EXISTS products;")
except:
    spark.sql("DROP TABLE IF EXISTS products;")

try:
    spark.sql("DROP VIEW IF EXISTS events;")
except:
    spark.sql("DROP TABLE IF EXISTS events;")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VIEW IF NOT EXISTS users AS SELECT * FROM delta.`s3a://dbx-data-public/v03/ecommerce/users/users.delta`;
# MAGIC CREATE VIEW IF NOT EXISTS sales AS SELECT * FROM delta.`s3a://dbx-data-public/v03/ecommerce/sales/sales.delta`;
# MAGIC CREATE VIEW IF NOT EXISTS products AS SELECT * FROM delta.`s3a://dbx-data-public/v03/products/products.delta`;
# MAGIC CREATE VIEW IF NOT EXISTS events AS SELECT * FROM delta.`s3a://dbx-data-public/v03/ecommerce/events/events.delta`;
# MAGIC

# COMMAND ----------

setup_spark_conf()

# COMMAND ----------

displayHTML("✅ Classroom setup complete! 🎉")
displayHTML(f"<br/>")
displayHTML(f"✅ Created database 'ceu'")
displayHTML(f"<br/>")
displayHTML(f"✅ Created views: users, sales, product, events")


