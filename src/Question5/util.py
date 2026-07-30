from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

from pyspark.sql.functions import (
    col,
    avg,
    current_date
)


# ----------------------------------------
# Create Employee DataFrame
# ----------------------------------------

def create_employee_df(spark):

    employee_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("State", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("Age", IntegerType(), True)
    ])


    employee_data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]


    return spark.createDataFrame(
        employee_data,
        employee_schema
    )



# ----------------------------------------
# Create Department DataFrame
# ----------------------------------------

def create_department_df(spark):

    department_schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])


    department_data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]


    return spark.createDataFrame(
        department_data,
        department_schema
    )



# ----------------------------------------
# Create Country DataFrame
# ----------------------------------------

def create_country_df(spark):

    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])


    country_data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]


    return spark.createDataFrame(
        country_data,
        country_schema
    )



# ----------------------------------------
# 2. Average Salary Department Wise
# ----------------------------------------

def average_salary(employee_df):

    return (
        employee_df
        .groupBy("department")
        .agg(
            avg("salary")
            .alias("avg_salary")
        )
    )



# ----------------------------------------
# 3. Employee Name Starts With m
# ----------------------------------------

def employee_start_m(employee_df, department_df):

    return (
        employee_df
        .join(
            department_df,
            employee_df.department == department_df.dept_id,
            "inner"
        )
        .filter(
            col("employee_name")
            .startswith("m")
        )
        .select(
            "employee_name",
            "dept_name"
        )
    )



# ----------------------------------------
# 4. Add Bonus Column
# ----------------------------------------

def add_bonus(employee_df):

    return (
        employee_df
        .withColumn(
            "bonus",
            col("salary") * 2
        )
    )



# ----------------------------------------
# 5. Reorder Columns
# ----------------------------------------

def reorder_columns(employee_df):

    return (
        employee_df
        .select(
            "employee_id",
            "employee_name",
            "salary",
            "State",
            "Age",
            "department"
        )
    )



# ----------------------------------------
# 6. Dynamic Join
# ----------------------------------------

def dynamic_join(
    employee_df,
    department_df,
    join_type
):

    return (
        employee_df
        .join(
            department_df,
            employee_df.department == department_df.dept_id,
            join_type
        )
    )



# ----------------------------------------
# 7. Replace State With Country Name
# ----------------------------------------

def replace_state_with_country(
    employee_df,
    country_df
):

    return (
        employee_df
        .join(
            country_df,
            employee_df.State == country_df.country_code,
            "left"
        )
        .drop("country_code")
        .drop("State")
        .withColumnRenamed(
            "country_name",
            "State"
        )
    )



# ----------------------------------------
# 8. Convert Column Names To Lowercase
# ----------------------------------------

def convert_lowercase(df):

    for column in df.columns:

        df = df.withColumnRenamed(
            column,
            column.lower()
        )

    return df



# ----------------------------------------
# Add Load Date Column
# ----------------------------------------

def add_load_date(df):

    return (
        df
        .withColumn(
            "load_date",
            current_date()
        )
    )



# ----------------------------------------
# 9. Write External Files
# ----------------------------------------

def write_external_tables(df):

    parquet_path = (
        "/Volumes/workspace/sona/s1/employee_parquet"
    )


    csv_path = (
        "/Volumes/workspace/sona/s1/employee_csv"
    )


    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .save(parquet_path)
    )


    (
        df.write
        .mode("overwrite")
        .format("csv")
        .option(
            "header",
            True
        )
        .save(csv_path)
    )