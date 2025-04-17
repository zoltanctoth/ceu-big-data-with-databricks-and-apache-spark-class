# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Abandoned Carts Lab
# MAGIC Analyze data from an e-commerce website to identify users who have added items to their cart but did not complete the purchase.
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Get emails of converted users from transactions
# MAGIC 2. Join emails with user IDs
# MAGIC 3. Get cart item history for each user
# MAGIC 4. Join cart item history with emails
# MAGIC 5. Filter for emails with abandoned cart items
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame</a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>: **`collect_set`**, **`explode`**, **`lit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Setup
# MAGIC Run the cells below to create DataFrames **`sales_df`**, **`users_df`**, and **`events_df`**.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# sale transactions at BedBricks
sales_df = spark.read.format("delta").load(sales_path)
display(sales_df)

# COMMAND ----------

# user IDs and emails at BedBricks
users_df = spark.read.format("delta").load(users_path)
display(users_df)

# COMMAND ----------

# events logged on the BedBricks website
events_df = spark.read.format("delta").load(events_path)
display(events_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1: Get emails of converted users from transactions
# MAGIC - Select the **`email`** column in **`sales_df`** and remove duplicates
# MAGIC - Add a new column **`converted`** with the boolean **`True`** for all rows
# MAGIC
# MAGIC Save the result as **`converted_users_df`**.

# COMMAND ----------

# TODO
from pyspark.sql.functions import *

converted_users_df = sales_df.FILL_IN
display(converted_users_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### 1.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["email", "converted"]
expected_count = 210370

# Create test suite
suite = create_test_suite("1.1")

# Run tests
suite.test_equals(
    actual=converted_users_df.columns,
    expected=expected_columns,
    description="converted_users_df has the correct columns",
)

suite.test_equals(
    actual=converted_users_df.count(),
    expected=expected_count,
    description="converted_users_df has the correct number of rows",
)

suite.test_true(
    condition=converted_users_df.select(col("converted")).first()[0] == True,
    description="converted column is correct",
)

# Display results
suite.display_results()
assert suite.passed, "One or more tests failed."

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2: Join emails with user IDs
# MAGIC - Perform an outer join on **`converted_users_df`** and **`users_df`** with the **`email`** field
# MAGIC - Filter for users where **`email`** is not null
# MAGIC - Fill null values in **`converted`** as **`False`**
# MAGIC
# MAGIC Save the result as **`conversions_df`**.

# COMMAND ----------

# TODO
conversions_df = users_df.FILL_IN
display(conversions_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### 2.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["email", "user_id", "user_first_touch_timestamp", "converted"]
expected_count = 782749
expected_false_count = 572379

# Create test suite
suite = create_test_suite("2.1")

# Run tests
suite.test_equals(
    actual=conversions_df.columns,
    expected=expected_columns,
    description="Columns are correct",
)

suite.test_equals(
    actual=conversions_df.filter(col("email").isNull()).count(),
    expected=0,
    description="Email column contains no nulls",
)

suite.test_equals(
    actual=conversions_df.count(),
    expected=expected_count,
    description="There is the correct number of rows",
)

suite.test_equals(
    actual=conversions_df.filter(col("converted") == False).count(),
    expected=expected_false_count,
    description="There is the correct number of false entries in converted column",
)

# Display results
suite.display_results()
assert suite.passed, "One or more tests failed."

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3: Get cart item history for each user
# MAGIC - Explode the **`items`** field in **`events_df`** with the results replacing the existing **`items`** field
# MAGIC - Group by **`user_id`**
# MAGIC   - Collect a set of all **`items.item_id`** objects for each user and alias the column to "cart"
# MAGIC
# MAGIC Save the result as **`carts_df`**.

# COMMAND ----------

# TODO
carts_df = events_df.FILL_IN
display(carts_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### 3.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["user_id", "cart"]
expected_count = 488403

# Create test suite
suite = create_test_suite("3.1")

# Run tests
suite.test_equals(
    actual=carts_df.columns, expected=expected_columns, description="Incorrect columns"
)

suite.test_equals(
    actual=carts_df.count(),
    expected=expected_count,
    description="Incorrect number of rows",
)

suite.test_equals(
    actual=carts_df.select(col("user_id")).drop_duplicates().count(),
    expected=expected_count,
    description="Duplicate user_ids present",
)

# Display results
suite.display_results()
assert suite.passed, "One or more tests failed."

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 4: Join cart item history with emails
# MAGIC - Perform a left join on **`conversions_df`** and **`carts_df`** on the **`user_id`** field
# MAGIC
# MAGIC Save result as **`email_carts_df`**.

# COMMAND ----------

# TODO
email_carts_df = conversions_df.FILL_IN
display(email_carts_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### 4.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = [
    "user_id",
    "email",
    "user_first_touch_timestamp",
    "converted",
    "cart",
]
expected_count = 782749
expected_cart_null_count = 397799

# Create test suite
suite = create_test_suite("4.1")

# Run tests
suite.test_equals(
    actual=email_carts_df.columns,
    expected=expected_columns,
    description="Columns do not match",
)

suite.test_equals(
    actual=email_carts_df.count(),
    expected=expected_count,
    description="Counts do not match",
)

suite.test_equals(
    actual=email_carts_df.filter(col("cart").isNull()).count(),
    expected=expected_cart_null_count,
    description="Cart null counts incorrect from join",
)

# Display results
suite.display_results()
assert suite.passed, "One or more tests failed."

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 5: Filter for emails with abandoned cart items
# MAGIC - Filter **`email_carts_df`** for users where **`converted`** is False
# MAGIC - Filter for users with non-null carts
# MAGIC
# MAGIC Save result as **`abandoned_carts_df`**.

# COMMAND ----------

# TODO
abandoned_carts_df = email_carts_df.FILL_IN
display(abandoned_carts_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### 5.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = [
    "user_id",
    "email",
    "user_first_touch_timestamp",
    "converted",
    "cart",
]
expected_count = 204272

# Create test suite
suite = create_test_suite("5.1")

# Run tests
suite.test_equals(
    actual=abandoned_carts_df.columns,
    expected=expected_columns,
    description="Columns do not match",
)

suite.test_equals(
    actual=abandoned_carts_df.count(),
    expected=expected_count,
    description="Counts do not match",
)

# Display results
suite.display_results()
assert suite.passed, "One or more tests failed."

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 6: Bonus Activity
# MAGIC Plot number of abandoned cart items by product

# COMMAND ----------

# TODO
abandoned_items_df = abandoned_carts_df.FILL_IN
display(abandoned_items_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### 6.1: Check Your Work
# MAGIC
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

abandoned_items_df.count()

# COMMAND ----------

expected_columns = ["items", "count"]
expected_count = 12

# Create test suite
suite = create_test_suite("6.1")

# Run tests
suite.test_equals(
    actual=abandoned_items_df.count(),
    expected=expected_count,
    description="Counts do not match",
)

suite.test_equals(
    actual=abandoned_items_df.columns,
    expected=expected_columns,
    description="Columns do not match",
)

# Display results
suite.display_results()
assert suite.passed, "One or more tests failed."

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

# Clean up resources at the end of the notebook
cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC This courseware is built on top of the <a href="https://github.com/databricks-academy/apache-spark-programming-with-databricks-english">Official Databricks Spark Programming Course</a>.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
