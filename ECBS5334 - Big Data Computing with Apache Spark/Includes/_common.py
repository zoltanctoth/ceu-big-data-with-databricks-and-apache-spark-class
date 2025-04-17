# Databricks notebook source
# Simplified _common.py without DBAcademy dependencies

# COMMAND ----------


# Mount S3 bucket - Run this only if the bucket is not already mounted
def mount_s3_bucket():
    """Mount the S3 bucket to /mnt/data if not already mounted."""
    try:
        # Check if the mount point already exists
        dbutils.fs.ls("/mnt/data")
        print("The mount already exists!")
    except Exception:
        print("Mounting S3 bucket s3://dbx-data-public to /mnt/data...")
        # Mount the S3 bucket to /mnt/data
        dbutils.fs.mount("s3a://dbx-data-public", "/mnt/data")
        print("S3 bucket mounted successfully!")


# COMMAND ----------

# Define paths for data access
data_source_version = "v03"

# Core data paths
sales_path = f"/mnt/data/{data_source_version}/ecommerce/sales/sales.delta"
users_path = f"/mnt/data/{data_source_version}/ecommerce/users/users.delta"
events_path = f"/mnt/data/{data_source_version}/ecommerce/events/events.delta"
products_path = f"/mnt/data/{data_source_version}/products/products.delta"

# People dataset path
people_path = f"/mnt/data/{data_source_version}/people/people-with-dups.txt"

# Working directories - for lab exercises
working_dir = "/tmp/spark-course-working"
checkpoints_dir = "/tmp/spark-course-checkpoints"

# COMMAND ----------


# Set Spark configuration parameters so paths can be accessed via SQL
def setup_spark_conf():
    """Set Spark config parameters to access paths in SQL queries."""
    spark.conf.set("sales_path", sales_path)
    spark.conf.set("users_path", users_path)
    spark.conf.set("events_path", events_path)
    spark.conf.set("products_path", products_path)
    spark.conf.set("people_path", people_path)
    spark.conf.set("working_dir", working_dir)
    spark.conf.set("checkpoints_dir", checkpoints_dir)


# COMMAND ----------


# Simple validation function to test if operations were successful
def test_success(condition, success_message, failure_message):
    """Simple test function to validate operations."""
    if condition:
        print(f"✅ {success_message}")
        return True
    else:
        print(f"❌ {failure_message}")
        return False


# COMMAND ----------


# Simple function to create test suites
def create_test_suite(name):
    """Create a simple test suite for validating lab exercises."""
    return SimpleSuite(name)


class SimpleSuite:
    """A simplified test suite to replace DA.tests functionality."""

    def __init__(self, name):
        self.name = name
        self.tests = []
        self.passed = True

    def test(self, description, test_function):
        """Add a test with a custom test function."""
        result = test_function()
        self.tests.append((description, result))
        if not result:
            self.passed = False
        return result

    def test_equals(self, actual, expected, description):
        """Test if actual equals expected."""
        result = actual == expected
        self.tests.append((description, result))
        if not result:
            self.passed = False
            print(f"❌ {description} - Expected {expected}, got {actual}")
        else:
            print(f"✅ {description}")
        return result

    def test_true(self, condition, description):
        """Test if condition is True."""
        result = condition == True
        self.tests.append((description, result))
        if not result:
            self.passed = False
            print(f"❌ {description}")
        else:
            print(f"✅ {description}")
        return result

    def test_false(self, condition, description):
        """Test if condition is False."""
        result = condition == False
        self.tests.append((description, result))
        if not result:
            self.passed = False
            print(f"❌ {description}")
        else:
            print(f"✅ {description}")
        return result

    def test_length(self, collection, expected_length, description):
        """Test if collection has expected length."""
        actual_length = len(collection)
        result = actual_length == expected_length
        self.tests.append((description, result))
        if not result:
            self.passed = False
            print(
                f"❌ {description} - Expected length {expected_length}, got {actual_length}"
            )
        else:
            print(f"✅ {description}")
        return result

    def display_results(self):
        """Print test results summary."""
        total = len(self.tests)
        passed = sum(1 for _, result in self.tests if result)
        print(f"\n===== Test Results for {self.name} =====")
        print(f"Passed: {passed}/{total} tests")
        if self.passed:
            print("🎉 All tests passed!")
        else:
            print("❌ Some tests failed.")
        print("=====================================\n")
        return self.passed


# COMMAND ----------


# Simplified function to create working directory
def reset_working_dir():
    """Reset the working directory."""
    try:
        dbutils.fs.rm(working_dir, True)
    except:
        pass

    try:
        dbutils.fs.mkdirs(working_dir)
        print(f"Created working directory: {working_dir}")
    except:
        print(f"Failed to create working directory: {working_dir}")


# COMMAND ----------


# Cleanup function that can be called at the end of notebooks
def cleanup():
    """Clean up resources at the end of a notebook."""
    try:
        # Stop any active streams
        for stream in spark.streams.active:
            stream.stop()
        print("Stopped all active streams.")
    except:
        pass

    try:
        # Remove working directory
        dbutils.fs.rm(working_dir, True)
        print(f"Removed working directory: {working_dir}")
    except:
        pass
