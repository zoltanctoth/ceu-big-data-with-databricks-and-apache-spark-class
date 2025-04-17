# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Sort Day Lab
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Define a UDF to label the day of week
# MAGIC 1. Apply the UDF to label and sort by day of week
# MAGIC 1. Plot active users by day of week as a bar graph

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Start with a DataFrame of the average number of active users by day of week.
# MAGIC
# MAGIC This was the resulting **`df`** in a previous lab.

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, avg, col, date_format, to_date

df = (spark
      .read
      .format("delta")
      .load(DA.paths.events)
      .withColumn("ts", (col("event_timestamp") / 1e6).cast("timestamp"))
      .withColumn("date", to_date("ts"))
      .groupBy("date").agg(approx_count_distinct("user_id").alias("active_users"))
      .withColumn("day", date_format(col("date"), "E"))
      .groupBy("day").agg(avg(col("active_users")).alias("avg_users"))
     )

display(df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Define UDF to label day of week
# MAGIC
# MAGIC Use the **`label_day_of_week`** function provided below to create the UDF **`label_dow_udf`**

# COMMAND ----------

def label_day_of_week(day: str) -> str:
    dow = {"Mon": "1", "Tue": "2", "Wed": "3", "Thu": "4",
           "Fri": "5", "Sat": "6", "Sun": "7"}
    return dow.get(day) + "-" + day

# COMMAND ----------

# ANSWER
label_dow_udf = spark.udf.register("label_dow", label_day_of_week)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Apply UDF to label and sort by day of week
# MAGIC - Update the **`day`** column by applying the UDF and replacing this column
# MAGIC - Sort by **`day`**
# MAGIC - Plot as a bar graph

# COMMAND ----------

# ANSWER
final_df = (df
            .withColumn("day", label_dow_udf(col("day")))
            .sort("day")
           )
display(final_df)

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
