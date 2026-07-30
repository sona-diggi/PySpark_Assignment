from pyspark.sql import SparkSession

from src.question2.util import (
    create_dataframe,
    print_partitions,
    increase_partitions,
    decrease_partitions,
    add_masked_column
)


def main():

    spark = (
        SparkSession.builder
        .appName("Question2")
        .getOrCreate()
    )

    # Create DataFrame
    credit_card_df = create_dataframe(spark)

    print("Original Data")
    credit_card_df.show(truncate=False)

    # Original Partitions
    print("\nOriginal Partition Count")
    print_partitions(credit_card_df)

    original_partition = credit_card_df.rdd.getNumPartitions()

    # Increase partitions
    credit_card_df = increase_partitions(credit_card_df, 5)

    print("\nAfter Increasing Partitions")
    print_partitions(credit_card_df)

    # Decrease partitions
    credit_card_df = decrease_partitions(
        credit_card_df,
        original_partition
    )

    print("\nAfter Decreasing Partitions")
    print_partitions(credit_card_df)

    # Mask card number
    result_df = add_masked_column(credit_card_df)

    print("\nFinal Output")
    result_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()