# Databricks notebook source
# MAGIC %md # Distributed Count Example

# COMMAND ----------

# MAGIC %md Let's disable the Adaptive Query Executor (More on that later)

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# MAGIC %md How many bytes are in a partition?

# COMMAND ----------

spark.conf.get('spark.sql.files.maxPartitionBytes')

# COMMAND ----------

# MAGIC %md How many <a href='https://en.wikipedia.org/wiki/Megabyte' target='_blank'>MiBs</a> is this? 

# COMMAND ----------

int(spark.conf.get('spark.sql.files.maxPartitionBytes')[:-1]) / 1024 / 1024  

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed/2015_2_clickstream.tsv

# COMMAND ----------

df = spark.read.csv(
  '/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed/2015_2_clickstream.tsv',
  sep='\t',
  header=True
)

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
