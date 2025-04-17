# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# Read in the dataset for the lab, along with all functions

from pyspark.sql.functions import *

df = spark.read.format("delta").load(DA.paths.sales)
display(df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Extract item details from purchases
# MAGIC
# MAGIC - Explode the **`items`** field in **`df`** with the results replacing the existing **`items`** field
# MAGIC - Select the **`email`** and **`item.item_name`** fields
# MAGIC - Split the words in **`item_name`** into an array and alias the column to "details"
# MAGIC
# MAGIC Assign the resulting DataFrame to **`details_df`**.

# COMMAND ----------

# TODO

from pyspark.sql.functions import *

details_df = (df
              .withColumn("items", FILL_IN("items"))
              .FILL_IN("email", "items.item_name")
              .withColumn("details", FILL_IN(col("item_name"), " "))
             )
display(details_df)

# COMMAND ----------

# Run this cell to check your work
assert details_df.count() == 235911

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC So you can see that our **`details`** column is now an array containing the quality, size, and object type.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Extract size and quality options from mattress purchases
# MAGIC
# MAGIC - Filter **`details_df`** for records where **`details`** contains "Mattress"
# MAGIC - Add a **`size`** column by extracting the element at position 2
# MAGIC - Add a **`quality`** column by extracting the element at position 1
# MAGIC
# MAGIC Save the result as **`mattress_df`**.

# COMMAND ----------

# TODO

mattress_df = (details_df
               .FILL_IN(array_contains(col("details"), "Mattress"))
               .withColumn("size", element_at(col("details"), FILL_IN))
               .withColumn("quality", FILL_IN(col("details"), 1))
              )
display(mattress_df)

# COMMAND ----------

# Run this cell to check your work
assert mattress_df.count() == 208384

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Next we're going to do the same thing for pillow purchases.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 3. Extract size and quality options from pillow purchases
# MAGIC - Filter **`details_df`** for records where **`details`** contains "Pillow"
# MAGIC - Add a **`size`** column by extracting the element at position 1
# MAGIC - Add a **`quality`** column by extracting the element at position 2
# MAGIC
# MAGIC Note the positions of **`size`** and **`quality`** are switched for mattresses and pillows.
# MAGIC
# MAGIC Save result as **`pillow_df`**.

# COMMAND ----------

# TODO

pillow_df = (details_df
             .FILL_IN(array_contains(col("details"), "Pillow"))
             .withColumn("size", FILL_IN(col("details"), 1))
             .FILL_IN("quality", FILL_IN(col("details"), 2))
            )
display(pillow_df)

# COMMAND ----------

# Run this cell to check your work
assert pillow_df.count() == 27527

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 4. Combine data for mattress and pillows
# MAGIC
# MAGIC - Perform a union on **`mattress_df`** and **`pillow_df`** by column names
# MAGIC - Drop the **`details`** column
# MAGIC
# MAGIC Save the result as **`union_df`**.

# COMMAND ----------

# TODO

union_df = mattress_df.FILL_IN(pillow_df).FILL_IN("details")
display(union_df)

# COMMAND ----------

# Run this cell to check your work
assert union_df.count() == 235911

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 5. List all size and quality options bought by each user
# MAGIC
# MAGIC - Group rows in **`union_df`** by **`email`**
# MAGIC   - Collect the set of all items in **`size`** for each user and alias the column to "size options"
# MAGIC   - Collect the set of all items in **`quality`** for each user and alias the column to "quality options"
# MAGIC
# MAGIC Save the result as **`options_df`**.

# COMMAND ----------

# TODO

options_df = (union_df
              .FILL_IN("email")
              .agg(FILL_IN("size").alias("size options"),
                   FILL_IN("quality").alias("quality options"))
             )
display(options_df)

# COMMAND ----------

# Run this cell to check your work
assert options_df.count() == 210370

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Clean up classroom
# MAGIC
# MAGIC And lastly, we'll clean up the classroom.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC This courseware is built on top of the <a href="https://github.com/databricks-academy/apache-spark-programming-with-databricks-english">Official Databricks Spark Programming Course</a>.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
