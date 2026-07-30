from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def create_dataframe(spark):
    """
    Creates Credit Card DataFrame.
    """

    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    columns = ["card_number"]

    return spark.createDataFrame(data, columns)


def print_partitions(df):
    """
    Prints number of partitions.
    """
    print(f"Number of Partitions : {df.rdd.getNumPartitions()}")


def increase_partitions(df, num_partitions):
    """
    Increase partitions.
    """
    return df.repartition(num_partitions)


def decrease_partitions(df, num_partitions):
    """
    Decrease partitions.
    """
    return df.coalesce(num_partitions)


def mask_card(card_number):
    """
    Mask all digits except last four.
    """
    return "*" * (len(card_number) - 4) + card_number[-4:]


mask_udf = F.udf(mask_card, StringType())


def add_masked_column(df):
    """
    Adds masked_card_number column.
    """
    return df.withColumn(
        "masked_card_number",
        mask_udf(F.col("card_number"))
    )