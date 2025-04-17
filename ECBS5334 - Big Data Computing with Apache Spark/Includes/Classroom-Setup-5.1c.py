# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# Mount the S3 bucket
mount_s3_bucket()

# COMMAND ----------


@ValidationHelper.monkey_patch
def validate_1_1(self, df):
    suite = DA.tests.new("5.1c-1.1")

    suite.test_true(lambda: df.isStreaming, description="The query is streaming")

    columns = [
        "device",
        "ecommerce",
        "event_name",
        "event_previous_timestamp",
        "event_timestamp",
        "geo",
        "items",
        "traffic_source",
        "user_first_touch_timestamp",
        "user_id",
    ]
    suite.test_sequence(
        lambda: df.columns,
        expected_value=columns,
        test_column_order=False,
        description=f"DataFrame contains all {len(columns)} columns",
        hint="Found [[ACTUAL_VALUE]]",
    )

    suite.display_results()
    assert suite.passed, "One or more tests failed."


# COMMAND ----------


@ValidationHelper.monkey_patch
def validate_2_1(self, schema: StructType):
    suite = DA.tests.new("5.1c-2.1")
    suite.test_equals(
        lambda: type(schema),
        expected_value=StructType,
        description="Schema is of type StructType",
        hint="Found [[ACTUAL_VALUE]]",
    )

    suite.test_length(
        lambda: schema.fieldNames(),
        2,
        description="Schema contians two fields",
        hint="Found [[LEN_ACTUAL_VALUE]]: [[ACTUAL_VALUE]]",
    )

    suite.test_schema_field(lambda: schema, "traffic_source", "StringType", None)
    suite.test_schema_field(lambda: schema, "active_users", "LongType", None)

    suite.display_results()
    assert suite.passed, "One or more tests failed."


# COMMAND ----------


@ValidationHelper.monkey_patch
def validate_4_1(self, query):
    suite = DA.tests.new("5.1c-4.1")

    suite.test_true(lambda: query.isActive, description="The query is active")
    suite.test_equals(
        lambda: query.name,
        "active_users_by_traffic",
        description='The query name is "active_users_by_traffic".',
    )
    suite.test_equals(
        lambda: query.lastProgress["sink"]["description"],
        "MemorySink",
        description='The format is "MemorySink".',
        hint="Found [[ACTUAL_VALUE]]",
    )

    suite.display_results()
    assert suite.passed, "One or more tests failed."


# COMMAND ----------


@ValidationHelper.monkey_patch
def validate_6_1(self, query):
    suite = DA.tests.new("5.1c-6.1")

    suite.test_false(lambda: query.isActive, description="The query has been stopped")

    suite.display_results()
    assert suite.passed, "One or more tests failed."


# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

# Define paths directly instead of using DA.paths
sales_path = "/mnt/data/v03/ecommerce/sales/sales.delta"
users_path = "/mnt/data/v03/ecommerce/users/users.delta"
events_path = "/mnt/data/v03/ecommerce/events/events.delta"
products_path = "/mnt/data/v03/products/products.delta"

# Set these as spark configuration parameters so they can be accessed as ${var_name} in SQL
spark.conf.set("sales_path", sales_path)
spark.conf.set("users_path", users_path)
spark.conf.set("events_path", events_path)
spark.conf.set("products_path", products_path)

DA.conclude_setup()
