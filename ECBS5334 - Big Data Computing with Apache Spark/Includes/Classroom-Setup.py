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
# MAGIC DROP TABLE IF EXISTS sales;
# MAGIC DROP TABLE IF EXISTS users;
# MAGIC DROP TABLE IF EXISTS products;
# MAGIC DROP TABLE IF EXISTS events;
# MAGIC

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

