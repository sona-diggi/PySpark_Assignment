from pyspark.sql.types import *
from util import *

purchase_data = [
    (1, "iphone13"),
    (1, "dell i5 core"),
    (2, "iphone13"),
    (2, "dell i5 core"),
    (3, "iphone13"),
    (3, "dell i5 core"),
    (1, "dell i3 core"),
    (1, "hp i5 core"),
    (1, "iphone14"),
    (3, "iphone14"),
    (4, "iphone13")
]

purchase_schema = StructType([
    StructField("customer", IntegerType()),
    StructField("product_model", StringType())
])

purchase_df = spark.createDataFrame(
    purchase_data,
    purchase_schema
)

product_data = [
    ("iphone13",),
    ("dell i5 core",),
    ("dell i3 core",),
    ("hp i5 core",),
    ("iphone14",)
]

product_schema = StructType([
    StructField("product_model", StringType())
])

product_df = spark.createDataFrame(
    product_data,
    product_schema
)

print("Customers who bought only iphone13")
customers_only_iphone13(purchase_df).show()

print("Customers upgraded to iphone14")
customers_upgraded(purchase_df).show()

print("Customers who bought all products")
customers_all_products(
    purchase_df,
    product_df
).show()
