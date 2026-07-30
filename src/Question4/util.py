from pyspark.sql.functions import *
import re


def flatten_dataframe(df):

    return df.select(
        "id",
        "firstName",
        "lastName",
        col("department.deptId").alias("deptId"),
        col("department.deptName").alias("deptName"),
        "skills"
    )


def record_count(df):

    before_count = df.count()

    explode_df = df.withColumn(
        "skill",
        explode("skills")
    )

    after_count = explode_df.count()

    print("Count Before Explode :", before_count)
    print("Count After Explode :", after_count)
    print("Difference :", after_count - before_count)


def explode_example(df):

    df.withColumn(
        "skill",
        explode("skills")
    ).show(truncate=False)


def explode_outer_example(df):

    df.withColumn(
        "skill",
        explode_outer("skills")
    ).show(truncate=False)


def posexplode_example(df):

    df.select(
        "id",
        "firstName",
        posexplode("skills")
    ).show(truncate=False)


def filter_employee(df):

    return df.filter(
        col("id") == "0001"
    )


def camel_to_snake(column):

    return re.sub(
        r'(?<!^)(?=[A-Z])',
        '_',
        column
    ).lower()


def rename_columns(df):

    snake_df = df

    for column in snake_df.columns:

        snake_df = snake_df.withColumnRenamed(
            column,
            camel_to_snake(column)
        )

    return snake_df


def add_load_date(df):

    return df.withColumn(
        "load_date",
        current_date()
    )


def create_partition_columns(df):

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


def write_table(df):

    spark.sql(
        "create database if not exists employee"
    )

    current_year = df.select("year").first()[0]
    current_month = df.select("month").first()[0]
    current_day = df.select("day").first()[0]

    replace_condition = (
        f"year={current_year} "
        f"AND month={current_month} "
        f"AND day={current_day}"
    )

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
            replace_condition
        )
        .saveAsTable(
            "employee.employee_details"
        )
    )

    print("Table Created Successfully")