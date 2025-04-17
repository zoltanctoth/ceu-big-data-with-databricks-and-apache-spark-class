# Databricks notebook source
def __validate_libraries():
    import requests

    try:
        site = "https://github.com/databricks-academy/dbacademy"
        response = requests.get(site)
        error = f"Unable to access GitHub or PyPi resources (HTTP {response.status_code} for {site})."
        assert (
            response.status_code == 200
        ), '{error} Please see the "Troubleshooting | {section}" section of the "Version Info" notebook for more information.'.format(
            error=error, section="Cannot Install Libraries"
        )
    except Exception as e:
        if type(e) is AssertionError:
            raise e
        error = f"Unable to access GitHub or PyPi resources ({site})."
        raise AssertionError(
            '{error} Please see the "Troubleshooting | {section}" section of the "Version Info" notebook for more information.'.format(
                error=error, section="Cannot Install Libraries"
            )
        ) from e


def __install_libraries():
    global pip_command

    specified_version = f"v3.0.23"
    key = "dbacademy.library.version"
    version = spark.conf.get(key, specified_version)

    if specified_version != version:
        print(
            "** Dependency Version Overridden *******************************************************************"
        )
        print(
            f"* This course was built for {specified_version} of the DBAcademy Library, but it is being overridden via the Spark"
        )
        print(
            f'* configuration variable "{key}". The use of version v3.0.23 is not advised as we'
        )
        print(f"* cannot guarantee compatibility with this version of the course.")
        print(
            "****************************************************************************************************"
        )

    try:
        from dbacademy import dbgems

        installed_version = dbgems.lookup_current_module_version("dbacademy")
        if installed_version == version:
            pip_command = (
                "list --quiet"  # Skipping pip install of pre-installed python library
            )
        else:
            print(
                f"WARNING: The wrong version of dbacademy is attached to this cluster. Expected {version}, found {installed_version}."
            )
            print(f"Installing the correct version.")
            raise Exception("Forcing re-install")

    except Exception as e:
        # The import fails if library is not attached to cluster
        if not version.startswith("v"):
            library_url = (
                f"git+https://github.com/databricks-academy/dbacademy@{version}"
            )
        else:
            library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"

        default_command = f"install --quiet --disable-pip-version-check {library_url}"
        pip_command = spark.conf.get("dbacademy.library.install", default_command)

        if pip_command != default_command:
            print(
                f"WARNING: Using alternative library installation:\n| default: %pip {default_command}\n| current: %pip {pip_command}"
            )
        else:
            # We are using the default libraries; next we need to verify that we can reach those libraries.
            __validate_libraries()


__install_libraries()

# COMMAND ----------

# MAGIC %pip $pip_command

# COMMAND ----------

# MAGIC %run ./_dataset_index

# COMMAND ----------

from dbacademy import dbgems
from dbacademy.dbhelper import CourseConfig, DBAcademyHelper, LessonConfig, Paths
from dbacademy.dbhelper.validations.validation_helper_class import ValidationHelper
from pyspark.sql.types import StructType


# Mount S3 bucket - Run this only if the bucket is not already mounted
def mount_s3_bucket():
    try:
        # Check if the mount point already exists
        dbutils.fs.ls("/mnt/data")
        print("The mount already exists!")
    except Exception:
        print("Mounting S3 bucket s3://dbx-data-public to /mnt/data...")
        # Mount the S3 bucket to /mnt/data
        dbutils.fs.mount("s3a://dbx-data-public", "/mnt/data")
        print("S3 bucket mounted successfully!")


# The following attributes are externalized to make them easy
# for content developers to update with every new course.

course_config = CourseConfig(
    course_code="asp",
    course_name="apache-spark-programming-with-databricks",
    data_source_name="apache-spark-programming-with-databricks",
    data_source_version="v03",
    install_min_time="2 min",
    install_max_time="5 min",
    remote_files=remote_files,
    supported_dbrs=[
        "11.3.x-scala2.12",
        "11.3.x-photon-scala2.12",
        "11.3.x-cpu-ml-scala2.12",
    ],
    expected_dbrs="11.3.x-scala2.12, 11.3.x-photon-scala2.12, 11.3.x-cpu-ml-scala2.12",
)

lesson_config = LessonConfig(
    name=None,
    create_schema=True,
    create_catalog=False,
    requires_uc=False,
    installing_datasets=True,
    enable_streaming_support=True,
    enable_ml_support=False,
)
