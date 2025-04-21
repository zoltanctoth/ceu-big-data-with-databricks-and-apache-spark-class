# Databricks notebook source
# MAGIC %md # Distributed Count Example

# COMMAND ----------

# MAGIC %md Let's disable the Adaptive Query Executor (More on that later)

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# MAGIC %md How many bytes are in a partition (maximum)?

# COMMAND ----------

max_bytes_in_part = spark.conf.get('spark.sql.files.maxPartitionBytes')
print(f"Max bytes in a partition: {max_bytes_in_part}")

max_mib_in_part = int(spark.conf.get('spark.sql.files.maxPartitionBytes')[:-1]) / 1024 / 1024  
print(f"This is {max_mib_in_part} negabytes")

# COMMAND ----------

# MAGIC %md How many cores do we have in the cluster?

# COMMAND ----------

print(f"Number of cores: {spark.sparkContext.defaultParallelism}")

# COMMAND ----------

file_path = "s3a://dbx-data-public/v03/ecommerce/events/events-1m.json/part-00000-tid-6289868722686892311-494bee2e-042e-46ab-b686-556a5ad5e3c1-2347-1-c000.json"

# COMMAND ----------

display(dbutils.fs.ls(file_path))

# COMMAND ----------

print(dbutils.fs.head(file_path).replace("\\n","\n"))

# COMMAND ----------

df = spark.read.json(file_path)

# COMMAND ----------

# MAGIC %md Guess: How many partitions?

# COMMAND ----------

# df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md Guess: How many jobs? How many stages? How many tasks?

# COMMAND ----------

df.count()

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled', 'true')

# COMMAND ----------

df.count()
