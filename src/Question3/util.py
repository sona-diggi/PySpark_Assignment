from pyspark.sql.functions import *


def rename_columns(df):

    column_mapping = {
        "log id": "log_id",
        "user$id": "user_id",
        "action": "user_activity",
        "timestamp": "time_stamp"
    }

    for old_col, new_col in column_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)

    return df


def convert_to_timestamp(df):

    return df.withColumn(
        "time_stamp",
        to_timestamp("time_stamp")
    )


def user_actions_last_7_days(df):

    latest_date = df.select(
        max("time_stamp")
    ).first()[0]

    cutoff_date = date_sub(lit(latest_date).cast("date"), 7)

    return (
        df.filter(col("time_stamp") >= cutoff_date)
          .groupBy("user_id")
          .count()
          .withColumnRenamed("count", "total_actions")
    )


def create_login_date(df):

    return df.withColumn(
        "login_date",
        to_date("time_stamp")
    )


def write_csv(df):

    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .option("delimiter", ",")
        .csv("/Volumes/workspace/sona/s1/login_details")
    )


def write_managed_table(df):

    (
        df.write
        .mode("overwrite")
        .saveAsTable("workspace.sona.login_details")
    )