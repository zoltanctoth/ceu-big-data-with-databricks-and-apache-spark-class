# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Explore Datasets Lab
# MAGIC
# MAGIC We'll use tools introduced in this lesson to explore the datasets used in this course.
# MAGIC
# MAGIC ### BedBricks Case Study
# MAGIC This course uses a case study that explores clickstream data for the online mattress retailer, BedBricks.
# MAGIC You are an analyst at BedBricks working with the following datasets: **`events`**, **`sales`**, **`users`**, and **`products`**.
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Get familiar with the data directory
# MAGIC 1. Create tables to access BedBricks sales data
# MAGIC 1. Execute SQL to answer basic questions about the data

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Get familiar with the data directory
# MAGIC
# MAGIC Let's take some time to explore the data directory and its contents to understand what files are available for us to use.

# COMMAND ----------

# ANSWER
files = dbutils.fs.ls("/mnt/data/v03/")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC The **`products`** directory contains data for products sold by our store.
# MAGIC
# MAGIC Let's explore this directory.

# COMMAND ----------

# ANSWER
products_files = dbutils.fs.ls("/mnt/data/v03/products")
display(products_files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC This directory has multiple data files. Let's look at the first one to see what type of information is stored about products.
# MAGIC
# MAGIC We can use the **`head`** command to preview the file.

# COMMAND ----------

# ANSWER
dbutils.fs.head(
    "/mnt/data/v03/products/products.delta/part-00000-8205eeb7-4264-4a62-afdb-b7f04ce8bc01-c000.snappy.parquet"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Create tables to access BedBricks sales data
# MAGIC
# MAGIC Let's create tables to access the BedBricks sales data. We'll need to create tables for:
# MAGIC - products
# MAGIC - sales
# MAGIC - events
# MAGIC - users
# MAGIC
# MAGIC Use the **`USING DELTA`** syntax and specify the path with the **`LOCATION`** keyword.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC CREATE TABLE IF NOT EXISTS users USING delta OPTIONS (path "${users_path}");
# MAGIC CREATE TABLE IF NOT EXISTS sales USING delta OPTIONS (path "${sales_path}");
# MAGIC CREATE TABLE IF NOT EXISTS products USING delta OPTIONS (path "${products_path}");
# MAGIC CREATE TABLE IF NOT EXISTS events USING delta OPTIONS (path "${events_path}");

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Use the data tab of the workspace UI to confirm your tables were created.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 4. Execute SQL to explore BedBricks datasets
# MAGIC Run SQL queries on the **`products`**, **`sales`**, and **`events`** tables to answer the following questions.
# MAGIC - What products are available for purchase at BedBricks?
# MAGIC - What is the average purchase revenue for a transaction at BedBricks?
# MAGIC - What types of events are recorded on the BedBricks website?
# MAGIC
# MAGIC The schema of the relevant dataset is provided for each question in the cells below.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### 4.1: What products are available for purchase at BedBricks?
# MAGIC
# MAGIC The **`products`** dataset contains the ID, name, and price of products on the BedBricks retail site.
# MAGIC
# MAGIC | field | type | description
# MAGIC | --- | --- | --- |
# MAGIC | item_id | string | unique item identifier |
# MAGIC | name | string | item name in plain text |
# MAGIC | price | double | price of item |
# MAGIC
# MAGIC Execute a SQL query that selects all from the **`products`** table.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see 12 products.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT * FROM products

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### 4.2: What is the average purchase revenue for a transaction at BedBricks?
# MAGIC
# MAGIC The **`sales`** dataset contains order information representing successfully processed sales.
# MAGIC Most fields correspond directly with fields from the clickstream data associated with a sale finalization event.
# MAGIC
# MAGIC | field | type | description|
# MAGIC | --- | --- | --- |
# MAGIC | order_id | long | unique identifier |
# MAGIC | email | string | the email address to which sales configuration was sent |
# MAGIC | transaction_timestamp | long | timestamp at which the order was processed, recorded in milliseconds since epoch |
# MAGIC | total_item_quantity | long | number of individual items in the order |
# MAGIC | purchase_revenue_in_usd | double | total revenue from order |
# MAGIC | unique_items | long | number of unique products in the order |
# MAGIC | items | array | provided as a list of JSON data, which is interpreted by Spark as an array of structs |
# MAGIC
# MAGIC Execute a SQL query that computes the average **`purchase_revenue_in_usd`** from the **`sales`** table.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> The result should be **`1042.79`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT AVG(purchase_revenue_in_usd)
# MAGIC FROM sales

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### 4.3: What types of events are recorded on the BedBricks website?
# MAGIC
# MAGIC The **`events`** dataset contains two weeks worth of parsed JSON records, created by consuming updates to an operational database.
# MAGIC Records are received whenever: (1) a new user visits the site, (2) a user provides their email for the first time.
# MAGIC
# MAGIC | field | type | description|
# MAGIC | --- | --- | --- |
# MAGIC | device | string | operating system of the user device |
# MAGIC | user_id | string | unique identifier for user/session |
# MAGIC | user_first_touch_timestamp | long | first time the user was seen in microseconds since epoch |
# MAGIC | traffic_source | string | referral source |
# MAGIC | geo (city, state) | struct | city and state information derived from IP address |
# MAGIC | event_timestamp | long | event time recorded as microseconds since epoch |
# MAGIC | event_previous_timestamp | long | time of previous event in microseconds since epoch |
# MAGIC | event_name | string | name of events as registered in clickstream tracker |
# MAGIC | items (item_id, item_name, price_in_usd, quantity, item_revenue in usd, coupon)| array | an array of structs for each unique item in the user's cart |
# MAGIC | ecommerce (total_item_quantity, unique_items, purchase_revenue_in_usd)  |  struct  | purchase data (this field is only non-null in those events that correspond to a sales finalization) |
# MAGIC
# MAGIC Execute a SQL query that selects distinct values in **`event_name`** from the **`events`** table
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see 23 distinct **`event_name`** values.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT DISTINCT event_name
# MAGIC FROM events

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

# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
