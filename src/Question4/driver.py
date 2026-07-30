from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType
)

path = "/Volumes/workspace/sona/s1/NestedEmp.json"

schema = StructType([
    StructField("id", StringType(), True),

    StructField("firstName", StringType(), True),

    StructField("lastName", StringType(), True),

    StructField(
        "department",
        StructType([
            StructField("deptId", IntegerType(), True),
            StructField("deptName", StringType(), True)
        ]),
        True
    ),

    StructField(
        "skills",
        ArrayType(StringType()),
        True
    )
])

employee_df = (
    spark.read
    .schema(schema)
    .option("multiline", True)
    .json(path)
)