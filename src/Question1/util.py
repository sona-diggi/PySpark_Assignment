from pyspark.sql.functions import *


def customers_only_iphone13(df):
    return (
        df.groupBy("customer")
        .agg(collect_set("product_model").alias("products"))
        .filter(
            (size(col("products")) == 1)
            & (array_contains(col("products"), "iphone13"))
        )
        .select("customer")
    )


def customers_upgraded(df):
    return (
        df.groupBy("customer")
        .agg(collect_set("product_model").alias("products"))
        .filter(
            array_contains(col("products"), "iphone13")
            & array_contains(col("products"), "iphone14")
        )
        .select("customer")
    )


def customers_all_products(purchase_df, product_df):

    total_products = product_df.count()

    valid_purchase = purchase_df.join(
        product_df,
        on="product_model",
        how="inner"
    )

    return (
        valid_purchase.groupBy("customer")
        .agg(
            countDistinct("product_model").alias("product_count")
        )
        .filter(col("product_count") == total_products)
        .select("customer")
    )