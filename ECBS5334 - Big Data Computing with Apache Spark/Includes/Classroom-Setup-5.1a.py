# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# Mount the S3 bucket
mount_s3_bucket()

# COMMAND ----------


@ValidationHelper.monkey_patch
def validate_1_1(self, df):
    suite = DA.tests.new("5.1a-1.1")

    suite.test_true(
        actual_value=lambda: df.isStreaming, description="The query is streaming"
    )

    columns = [
        "order_id",
        "email",
        "transaction_timestamp",
        "total_item_quantity",
        "purchase_revenue_in_usd",
        "unique_items",
        "items",
    ]
    suite.test_sequence(
        actual_value=lambda: df.columns,
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

    suite = DA.tests.new("5.1a-2.1")

    suite.test_equals(
        actual_value=lambda: type(schema),
        expected_value=StructType,
        description="Schema is of type StructType",
        hint="Found [[ACTUAL_VALUE]]",
    )

    suite.test_length(
        lambda: schema.fieldNames(),
        expected_length=7,
        description="Schema contians seven fields",
        hint="Found [[LEN_ACTUAL_VALUE]]: [[ACTUAL_VALUE]]",
    )

    suite.test_schema_field(lambda: schema, "order_id", "LongType", None)
    suite.test_schema_field(lambda: schema, "email", "StringType", None)
    suite.test_schema_field(lambda: schema, "transaction_timestamp", "LongType", None)
    suite.test_schema_field(lambda: schema, "total_item_quantity", "LongType", None)
    suite.test_schema_field(
        lambda: schema, "purchase_revenue_in_usd", "DoubleType", None
    )
    suite.test_schema_field(lambda: schema, "unique_items", "LongType", None)
    suite.test_schema_field(lambda: schema, "items", "StructType", None)

    suite.display_results()
    assert suite.passed, "One or more tests failed."


# COMMAND ----------


@ValidationHelper.monkey_patch
def validate_3_1(self, query):
    suite = DA.tests.new("5.1a-3.1")

    suite.test_true(
        actual_value=lambda: query.isActive, description="The query is active"
    )

    suite.test_equals(
        lambda: coupon_sales_query.lastProgress["name"],
        "coupon_sales",
        description='The query name is "coupon_sales".',
    )

    # Define working_dir and checkpoints directly instead of using DA.paths
    working_dir = "/tmp/working_dir"
    checkpoints_dir = "/tmp/checkpoints"

    coupons_output_path = f"{working_dir}/coupon-sales/output"
    suite.test(
        actual_value=lambda: None,
        test_function=lambda: len(dbutils.fs.ls(coupons_output_path)) > 0,
        description=f"Found at least one file in .../coupon-sales/output",
    )

    coupons_checkpoint_path = f"{checkpoints_dir}/coupon-sales"
    suite.test(
        actual_value=lambda: None,
        test_function=lambda: len(dbutils.fs.ls(coupons_checkpoint_path)) > 0,
        description=f"Found at least one file in .../coupon-sales",
    )

    suite.display_results()
    assert suite.passed, "One or more tests failed."


# COMMAND ----------


@ValidationHelper.monkey_patch
def validate_4_1(self, query_id, query_status):
    suite = DA.tests.new("5.1a-4.1")

    suite.test_sequence(
        actual_value=lambda: query_status.keys(),
        expected_value=["message", "isDataAvailable", "isTriggerActive"],
        test_column_order=False,
        description="Valid status value.",
    )

    suite.test_equals(lambda: type(query_id), str, description="Valid query_id value.")

    suite.display_results()
    assert suite.passed, "One or more tests failed."


# COMMAND ----------


@ValidationHelper.monkey_patch
def validate_5_1(self, query):
    suite = DA.tests.new("5.1a-5.1")

    suite.test_false(
        actual_value=lambda: query.isActive, description="The query is not active"
    )

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
