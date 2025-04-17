# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Ingesting Data Lab
# MAGIC
# MAGIC Read in CSV files containing products data.
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Read with infer schema
# MAGIC 3. Read with schema as DDL formatted string
# MAGIC 4. Write using Delta format

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Read with infer schema
# MAGIC - View the first CSV file using DBUtils method **`fs.head`** with the filepath provided in the variable **`single_product_cs_file_path`**
# MAGIC - Create **`products_df`** by reading from CSV files located in the filepath provided in the variable **`products_csv_path`**
# MAGIC   - Configure options to use first line as header and infer schema

# COMMAND ----------

# TODO
single_product_csv_file_path = f"{DA.paths.datasets}/products/products.csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
print(FILL_IN)

products_csv_path = f"{DA.paths.datasets}/products/products.csv"
products_df = FILL_IN

products_df.printSchema()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

assert(products_df.count() == 12)
print("All test pass")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Read with DDL formatted string

# COMMAND ----------

# TODO
ddl_schema = FILL_IN

products_df3 = FILL_IN

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

assert(products_df3.count() == 12)
print("All test pass")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 3. Write to Delta
# MAGIC Write **`products_df`** to the filepath provided in the variable **`products_output_path`**

# COMMAND ----------

# TODO
products_output_path = f"{DA.paths.working_dir}/delta/products"
products_df.FILL_IN

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**

# COMMAND ----------

verify_files = dbutils.fs.ls(products_output_path)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files > 0, "No data written"
del verify_files, verify_delta_format, verify_num_data_files
print("All test pass")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC This courseware is built on top of the <a href="https://github.com/databricks-academy/apache-spark-programming-with-databricks-english">Official Databricks Spark Programming Course</a>.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
