import re
 
from pyspark.sql.functions import (
    col,
    explode,
    explode_outer,
    posexplode,
    current_date,
    year,
    month,
    dayofmonth
)
 
 
# -----------------------------------
# Read JSON File Dynamically
# -----------------------------------
 
def read_json_dynamic(spark, path):
 
    return (
        spark.read
        .option("multiline", "true")
        .json(path)
    )
 
 
# -----------------------------------
# Flatten Nested JSON
# -----------------------------------
 
def flatten_df(df):
 
    return (
        df
        .withColumn(
            "employee",
            explode("employees")
        )
        .select(
            col("id"),
            col("properties.name")
            .alias("companyName"),
 
            col("properties.storeSize")
            .alias("storeSize"),
 
            col("employee.empId")
            .alias("employeeId"),
 
            col("employee.empName")
            .alias("employeeName")
        )
    )
 
 
# -----------------------------------
# Record Count
# -----------------------------------
 
def record_count(df):
 
    return df.count()
 
 
# -----------------------------------
# explode()
# -----------------------------------
 
def explode_demo(df):
 
    return (
        df
        .select(
            "id",
            explode("employees")
            .alias("employee")
        )
    )
 
 
# -----------------------------------
# explode_outer()
# -----------------------------------
 
def explode_outer_demo(df):
 
    return (
        df
        .select(
            "id",
            explode_outer("employees")
            .alias("employee")
        )
    )
 
 
# -----------------------------------
# posexplode()
# -----------------------------------
 
def posexplode_demo(df):
 
    return (
        df
        .select(
            "id",
            posexplode("employees")
        )
    )
 
 
# -----------------------------------
# Filter Data
# -----------------------------------
 
def filter_id(df):
 
    return (
        df
        .filter(
            col("id") == 1001
        )
    )
 
 
# -----------------------------------
# Camel Case to Snake Case
# -----------------------------------
 
def camel_to_snake(name):
 
    return (
        re.sub(
            r'(?<!^)(?=[A-Z])',
            '_',
            name
        )
        .lower()
    )
 
 
# -----------------------------------
# Rename Columns
# -----------------------------------
 
def rename_columns_snake(df):
 
    for column in df.columns:
 
        df = df.withColumnRenamed(
            column,
            camel_to_snake(column)
        )
 
    return df
 
 
# -----------------------------------
# Add Load Date
# -----------------------------------
 
def add_load_date(df):
 
    return (
        df
        .withColumn(
            "load_date",
            current_date()
        )
    )
 
 
# -----------------------------------
# Add Partition Columns
# -----------------------------------
 
def add_partition_columns(df):
 
    return (
        df
        .withColumn(
            "year",
            year("load_date")
        )
        .withColumn(
            "month",
            month("load_date")
        )
        .withColumn(
            "day",
            dayofmonth("load_date")
        )
    )
 
 
# -----------------------------------
# Write JSON Table
# Database : employee
# Table    : employee_details
# Partition: year, month, day
# -----------------------------------
 
def write_partitioned_table(df):
 
    (
        df.write
        .mode("overwrite")
        .format("json")
        .partitionBy(
            "year",
            "month",
            "day"
        )
        .option(
            "replaceWhere",
            "year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL"
        )
        .save(
            "/Volumes/workspace/sona/s1/nested_json_file.json"
        )
    )
 