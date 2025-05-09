# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# Set up spark configuration for SQL access to data paths
setup_spark_conf()

# COMMAND ----------

# Reset working directory for lab exercises
reset_working_dir()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ceu;
# MAGIC USE ceu;
# MAGIC
# MAGIC

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

displayHTML("✅ Classroom setup complete! 🎉")
displayHTML(f"<br/>")
displayHTML(f"✅ Created database 'ceu'")
displayHTML(f"✅ Created working directory: {working_dir} 📁")
displayHTML(f"<br/>")
displayHTML(f"Data paths:")
displayHTML(f"- Users data: {users_path}")
displayHTML(f"- Events data: {events_path}")
displayHTML(f"- Products data: {products_path}")
displayHTML(f"- Sales data: {sales_path}")
