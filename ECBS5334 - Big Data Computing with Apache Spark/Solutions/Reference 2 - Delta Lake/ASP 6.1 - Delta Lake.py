# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Delta Lake
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Create a Delta Table
# MAGIC 1. Understand the transaction Log
# MAGIC 1. Read data from your Delta Table
# MAGIC 1. Update data in your Delta Table
# MAGIC 1. Access previous versions of table using time travel
# MAGIC 1. Vacuum
# MAGIC
# MAGIC ##### Documentation
# MAGIC - <a href="https://docs.delta.io/latest/quick-start.html#create-a-table" target="_blank">Delta Table</a> 
# MAGIC - <a href="https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html" target="_blank">Transaction Log</a> 
# MAGIC - <a href="https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html" target="_blank">Time Travel</a>

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Create a Delta Table
# MAGIC Let's first read the Parquet-format BedBricks events dataset.

# COMMAND ----------

events_df = spark.read.format("parquet").load(f"{DA.paths.datasets}/ecommerce/events/events.parquet")
display(events_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Write the data in Delta format to the directory given by **`delta_path`**.

# COMMAND ----------

delta_path = f"{DA.paths.working_dir}/delta-events"
events_df.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Write the data in Delta format as a managed table in the metastore.

# COMMAND ----------

events_df.write.format("delta").mode("overwrite").saveAsTable("delta_events")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC As with other file formats, Delta supports partitioning your data in storage using the unique values in a specified column (often referred to as "Hive partitioning").
# MAGIC
# MAGIC Let's **overwrite** the Delta dataset in the **`delta_path`** directory to partition by state. This can accelerate queries that filter by state.

# COMMAND ----------

from pyspark.sql.functions import col

state_events_df = events_df.withColumn("state", col("geo.state"))

state_events_df.write.format("delta").mode("overwrite").partitionBy("state").option("overwriteSchema", "true").save(delta_path)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Understand the Transaction Log
# MAGIC We can see how Delta stores the different state partitions in separate directories.
# MAGIC
# MAGIC Additionally, we can also see a directory called **`_delta_log`**, which is the transaction log.
# MAGIC
# MAGIC When a Delta Lake dataset is created, its transaction log is automatically created in the **`_delta_log`** subdirectory.

# COMMAND ----------

display(dbutils.fs.ls(delta_path))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC When changes are made to that table, these changes are recorded as ordered, atomic commits in the transaction log.
# MAGIC
# MAGIC Each commit is written out as a JSON file, starting with 00000000000000000000.json.
# MAGIC
# MAGIC Additional changes to the table generate subsequent JSON files in ascending numerical order.
# MAGIC
# MAGIC <div style="img align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://user-images.githubusercontent.com/20408077/87174138-609fe600-c29c-11ea-90cc-84df0c1357f1.png" width="500"/>
# MAGIC </div>

# COMMAND ----------

display(dbutils.fs.ls(f"{delta_path}/_delta_log/"))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Next, let's take a look at a transaction log File.
# MAGIC
# MAGIC
# MAGIC The <a href="https://docs.databricks.com/delta/delta-utility.html" target="_blank">four columns</a> each represent a different part of the very first commit to the Delta Table, creating the table.
# MAGIC - The **`add`** column has statistics about the DataFrame as a whole and individual columns.
# MAGIC - The **`commitInfo`** column has useful information about what the operation was (WRITE or READ) and who executed the operation.
# MAGIC - The **`metaData`** column contains information about the column schema.
# MAGIC - The **`protocol`** version contains information about the minimum Delta version necessary to either write or read to this Delta Table.

# COMMAND ----------

display(spark.read.json(f"{delta_path}/_delta_log/00000000000000000000.json"))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC One key difference between these two transaction logs is the size of the JSON file, this file has 206 rows compared to the previous 7.
# MAGIC
# MAGIC To understand why, let's take a look at the **`commitInfo`** column. We can see that in the **`operationParameters`** section, **`partitionBy`** has been filled in by the **`state`** column. Furthermore, if we look at the add section on row 3, we can see that a new section called **`partitionValues`** has appeared. As we saw above, Delta stores partitions separately in memory, however, it stores information about these partitions in the same transaction log file.

# COMMAND ----------

display(spark.read.json(f"{delta_path}/_delta_log/00000000000000000001.json"))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Finally, let's take a look at the files inside one of the state partitions. The files inside corresponds to the partition commit (file 01) in the _delta_log directory.

# COMMAND ----------

display(dbutils.fs.ls(f"{delta_path}/state=CA/"))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Read from your Delta table

# COMMAND ----------

df = spark.read.format("delta").load(delta_path)
display(df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Update your Delta Table
# MAGIC
# MAGIC Let's filter for rows where the event takes place on a mobile device.

# COMMAND ----------

df_update = state_events_df.filter(col("device").isin(["Android", "iOS"]))
display(df_update)

# COMMAND ----------

df_update.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------

df = spark.read.format("delta").load(delta_path)
display(df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Let's look at the files in the California partition post-update. Remember, the different files in this directory are snapshots of your DataFrame corresponding to different commits.

# COMMAND ----------

display(dbutils.fs.ls(f"{delta_path}/state=CA/"))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Access previous versions of table using Time  Travel

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Oops, it turns out we actually we need the entire dataset! You can access a previous version of your Delta Table using Time Travel. Use the following two cells to access your version history. Delta Lake will keep a 30 day version history by default, but if necessary, Delta can store a version history for longer.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS train_delta")
spark.sql(f"CREATE TABLE train_delta USING DELTA LOCATION '{delta_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY train_delta

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Using the **`versionAsOf`** option allows you to easily access previous versions of our Delta Table.

# COMMAND ----------

df = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
display(df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC You can also access older versions using a timestamp.
# MAGIC
# MAGIC Replace the timestamp string with the information from your version history. 
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> Note: You can use a date without the time information if necessary.

# COMMAND ----------

# ANSWER

temp_df = spark.sql("DESCRIBE HISTORY train_delta").select("timestamp").orderBy(col("timestamp").asc())
time_stamp = temp_df.first()["timestamp"]

as_of_df = spark.read.format("delta").option("timestampAsOf", time_stamp).load(delta_path)
display(as_of_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Vacuum 
# MAGIC
# MAGIC Now that we're happy with our Delta Table, we can clean up our directory using **`VACUUM`**. Vacuum accepts a retention period in hours as an input.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC It looks like our code doesn't run! By default, to prevent accidentally vacuuming recent commits, Delta Lake will not let users vacuum a period under 7 days or 168 hours. Once vacuumed, you cannot return to a prior commit through time travel, only your most recent Delta Table will be saved.

# COMMAND ----------

# from delta.tables import *

# delta_table = DeltaTable.forPath(spark, delta_path)
# delta_table.vacuum(0)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC We can workaround this by setting a spark configuration that will bypass the default retention period check.

# COMMAND ----------

from delta.tables import *

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
delta_table = DeltaTable.forPath(spark, delta_path)
delta_table.vacuum(0)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Let's take a look at our Delta Table files now. After vacuuming, the directory only holds the partition of our most recent Delta Table commit.

# COMMAND ----------

display(dbutils.fs.ls(delta_path + "/state=CA/"))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Since vacuuming deletes files referenced by the Delta Table, we can no longer access past versions. 
# MAGIC
# MAGIC The code below should throw an error.
# MAGIC
# MAGIC Uncomment it and give it a try.

# COMMAND ----------

# df = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
# display(df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC This courseware is built on top of the <a href="https://github.com/databricks-academy/apache-spark-programming-with-databricks-english">Official Databricks Spark Programming Course</a>.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
