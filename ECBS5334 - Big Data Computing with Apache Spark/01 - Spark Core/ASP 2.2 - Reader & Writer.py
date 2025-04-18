# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Reader & Writer
# MAGIC ##### Objectives
# MAGIC 1. Read from CSV files
# MAGIC 1. Read from JSON files
# MAGIC 1. Write DataFrame to files
# MAGIC 1. Write DataFrame to tables
# MAGIC 1. Write DataFrame to a Delta table
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html" target="_blank">DataFrameReader</a>: **`csv`**, **`json`**, **`option`**, **`schema`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html" target="_blank">DataFrameWriter</a>: **`mode`**, **`option`**, **`parquet`**, **`format`**, **`saveAsTable`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html?highlight=structtype#pyspark.sql.types.StructType" target="_blank">StructType</a>: **`toDDL`**
# MAGIC
# MAGIC ##### Spark Types
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html" target="_blank">Types</a>: **`ArrayType`**, **`DoubleType`**, **`IntegerType`**, **`LongType`**, **`StringType`**, **`StructType`**, **`StructField`**

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## The `DA` object
# MAGIC To ease our access to different paths, the _Classroom Setup_ scripts include an object called `DA` that we can use for accessing paths and different properties of our workspace.

# COMMAND ----------

DA

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## DataFrameReader
# MAGIC Interface used to load a DataFrame from external storage systems
# MAGIC
# MAGIC **`spark.read.parquet("path/to/files")`**
# MAGIC
# MAGIC DataFrameReader is accessible through the SparkSession attribute **`read`**. This class includes methods to load DataFrames from different external storage systems.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Read from CSV files
# MAGIC Read from CSV with the DataFrameReader's **`csv`** method and the following options:
# MAGIC
# MAGIC Tab separator, use first line as header, infer schema

# COMMAND ----------

users_csv_path = f"{DA.paths.datasets}/ecommerce/users/users-500k.csv"

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .option("inferSchema", True)
           .csv(users_csv_path)
          )

users_df.printSchema()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Manually define the schema by creating a **`StructType`** with column names and data types

# COMMAND ----------

ddl_schema = "user_id string, user_first_touch_timestamp long, email string"

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .schema(ddl_schema)
           .csv(users_csv_path)
          )

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Read from JSON files
# MAGIC
# MAGIC Read from JSON with DataFrameReader's **`json`** method and the infer schema option

# COMMAND ----------

events_json_path = f"{DA.paths.datasets}/ecommerce/events/events-500k.json"

events_df = (spark
            .read
            .option("inferSchema", True)
            .json(events_json_path)
           )

events_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's do it with manual schema specification

# COMMAND ----------

events_schema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

# Step 6 - Read in the JSON data using our new DDL formatted string
events_df = (spark.read
                 .schema(events_schema)
                 .json(events_json_path))

display(events_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Read data by creating a **`StructType`** with the schema names and data types

# COMMAND ----------

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("device", StringType(), True),
    StructField("ecommerce", StructType([
        StructField("purchaseRevenue", DoubleType(), True),
        StructField("total_item_quantity", LongType(), True),
        StructField("unique_items", LongType(), True)
    ]), True),
    StructField("event_name", StringType(), True),
    StructField("event_previous_timestamp", LongType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("geo", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("items", ArrayType(
        StructType([
            StructField("coupon", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("item_revenue_in_usd", DoubleType(), True),
            StructField("price_in_usd", DoubleType(), True),
            StructField("quantity", LongType(), True)
        ])
    ), True),
    StructField("traffic_source", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("user_id", StringType(), True)
])

events_df = (spark
            .read
            .schema(user_defined_schema)
            .json(events_json_path)
           )

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## DataFrameWriter
# MAGIC Interface used to write a DataFrame to external storage systems
# MAGIC
# MAGIC <strong><code>
# MAGIC (df  
# MAGIC &nbsp;  .write                         
# MAGIC &nbsp;  .option("compression", "snappy")  
# MAGIC &nbsp;  .mode("overwrite")      
# MAGIC &nbsp;  .parquet(output_dir)       
# MAGIC )
# MAGIC </code></strong>
# MAGIC
# MAGIC DataFrameWriter is accessible through the SparkSession attribute **`write`**. This class includes methods to write DataFrames to different external storage systems.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Write DataFrames to files
# MAGIC
# MAGIC Write **`users_df`** to parquet with DataFrameWriter's **`parquet`** method and the following configurations:
# MAGIC
# MAGIC Snappy compression, overwrite mode

# COMMAND ----------

users_output_dir = f"{DA.paths.working_dir}/users.parquet"

(users_df
 .write
 .option("compression", "snappy")
 .mode("overwrite")
 .parquet(users_output_dir)
)

# COMMAND ----------

display(
    dbutils.fs.ls(users_output_dir)
)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Write DataFrames to tables
# MAGIC
# MAGIC Write **`events_df`** to a table using the DataFrameWriter method **`saveAsTable`**
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> This creates a global table, unlike the local view created by the DataFrame method **`createOrReplaceTempView`**

# COMMAND ----------

events_df.write.mode("overwrite").saveAsTable("events")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC This table was saved in the database created for you in classroom setup.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ceu

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Delta Lake
# MAGIC
# MAGIC In almost all cases, the best practice is to use Delta Lake format, especially whenever the data will be referenced from a Databricks workspace. 
# MAGIC
# MAGIC <a href="https://delta.io/" target="_blank">Delta Lake</a> is an open source technology designed to work with Spark to bring reliability to data lakes.
# MAGIC
# MAGIC ![delta](https://files.training.databricks.com/images/aspwd/delta_storage_layer.png)
# MAGIC
# MAGIC #### Delta Lake's Key Features
# MAGIC - ACID transactions
# MAGIC - Scalable metadata handling
# MAGIC - Unified streaming and batch processing
# MAGIC - Time travel (data versioning)
# MAGIC - Schema enforcement and evolution
# MAGIC - Audit history
# MAGIC - Parquet format
# MAGIC - Compatible with Apache Spark API

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Write Results to a Delta Table
# MAGIC
# MAGIC Write **`events_df`** with the DataFrameWriter's **`save`** method and the following configurations: Delta format & overwrite mode.

# COMMAND ----------

events_output_path = f"{DA.paths.working_dir}/delta/events"

(events_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(events_output_path)
)

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
# MAGIC This courseware is built on top of the <a href="https://github.com/databricks-academy/apache-spark-programming-with-databricks-english">Official Databricks Spark Programming Course</a>.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
