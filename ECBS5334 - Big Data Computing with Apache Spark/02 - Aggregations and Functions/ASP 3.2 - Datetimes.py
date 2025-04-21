# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Datetime Functions
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Cast to timestamp
# MAGIC 2. Format datetimes
# MAGIC 3. Extract from timestamp
# MAGIC 4. Convert to date
# MAGIC 5. Manipulate datetimes
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`cast`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions" target="_blank">Built-In Functions</a>: **`date_format`**, **`to_date`**, **`date_add`**, **`year`**, **`month`**, **`dayofweek`**, **`minute`**, **`second`**

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Let's use a subset of the BedBricks events dataset to practice working with date times.

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.format("delta").load(DA.paths.events).select("user_id", col("event_timestamp"))
display(df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Built-In Functions: Date Time Functions
# MAGIC Here are a few built-in functions to manipulate dates and times in Spark.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`add_months`** | Returns the date that is numMonths after startDate |
# MAGIC | **`current_timestamp`** | Returns the current timestamp at the start of query evaluation as a timestamp column |
# MAGIC | **`date_format`** | Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument. |
# MAGIC | **`dayofweek`** | Extracts the day of the month as an integer from a given date/timestamp/string |
# MAGIC | **`from_unixtime`** | Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the yyyy-MM-dd HH:mm:ss format |
# MAGIC | **`minute`** | Extracts the minutes as an integer from a given date/timestamp/string. |
# MAGIC | **`unix_timestamp`** | Converts time string with given pattern to Unix timestamp (in seconds) |

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Cast to Timestamp

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`cast()`**
# MAGIC Casts column to a different data type, specified using string representation or DataType.

# COMMAND ----------

timestamp_df = df.withColumn("event_timestamp", (col("event_timestamp") / 1e6).cast("timestamp"))
display(timestamp_df)

# COMMAND ----------

from pyspark.sql.types import TimestampType

timestamp_df = df.withColumn("event_timestamp", (col("event_timestamp") / 1e6).cast(TimestampType()))
display(timestamp_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Datetime Patterns for Formatting and Parsing
# MAGIC There are several common scenarios for datetime usage in Spark:
# MAGIC
# MAGIC - CSV/JSON datasources use the pattern string for parsing and formatting datetime content.
# MAGIC - Datetime functions related to convert StringType to/from DateType or TimestampType e.g. **`unix_timestamp`**, **`date_format`**, **`from_unixtime`**, **`to_date`**, **`to_timestamp`**, etc.
# MAGIC
# MAGIC Spark uses <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html" target="_blank">pattern letters for date and timestamp parsing and formatting</a>. A subset of these patterns are shown below.
# MAGIC
# MAGIC | Symbol | Meaning         | Presentation | Examples               |
# MAGIC | ------ | --------------- | ------------ | ---------------------- |
# MAGIC | G      | era             | text         | AD; Anno Domini        |
# MAGIC | y      | year            | year         | 2020; 20               |
# MAGIC | D      | day-of-year     | number(3)    | 189                    |
# MAGIC | M/L    | month-of-year   | month        | 7; 07; Jul; July       |
# MAGIC | d      | day-of-month    | number(3)    | 28                     |
# MAGIC | Q/q    | quarter-of-year | number/text  | 3; 03; Q3; 3rd quarter |
# MAGIC | E      | day-of-week     | text         | Tue; Tuesday           |
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Spark's handling of dates and timestamps changed in version 3.0, and the patterns used for parsing and formatting these values changed as well. For a discussion of these changes, please reference <a href="https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html" target="_blank">this Databricks blog post</a>.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Format date

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`date_format()`**
# MAGIC Converts a date/timestamp/string to a string formatted with the given date time pattern.

# COMMAND ----------

from pyspark.sql.functions import date_format

formatted_df = (timestamp_df
                .withColumn("date string", date_format("event_timestamp", "MMMM dd, yyyy"))
                .withColumn("time string", date_format("event_timestamp", "HH:mm:ss.SSSSSS"))
               )
display(formatted_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Extract datetime attribute from timestamp

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`year`**
# MAGIC Extracts the year as an integer from a given date/timestamp/string.
# MAGIC
# MAGIC ##### Similar methods: **`month`**, **`dayofweek`**, **`minute`**, **`second`**, etc.

# COMMAND ----------

from pyspark.sql.functions import dayofweek, minute, month, second, year

datetime_df = (timestamp_df
               .withColumn("year", year(col("event_timestamp")))
               .withColumn("month", month(col("event_timestamp")))
               .withColumn("dayofweek", dayofweek(col("event_timestamp")))
               .withColumn("minute", minute(col("event_timestamp")))
               .withColumn("second", second(col("event_timestamp")))
              )
display(datetime_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Convert to Date

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`to_date`**
# MAGIC Converts the column into DateType by casting rules to DateType.

# COMMAND ----------

from pyspark.sql.functions import to_date

date_df = timestamp_df.withColumn("date", to_date(col("event_timestamp")))
display(date_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Manipulate Datetimes

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### **`date_add`**
# MAGIC Returns the date that is the given number of days after start

# COMMAND ----------

from pyspark.sql.functions import date_add

plus_2_df = timestamp_df.withColumn("plus_two_days", date_add(col("event_timestamp"), 2))
display(plus_2_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC Licence: <a target='_blank' href='https://github.com/databricks-academy/apache-spark-programming-with-databricks/blob/published/LICENSE'>Creative Commons Zero v1.0 Universal</a>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
