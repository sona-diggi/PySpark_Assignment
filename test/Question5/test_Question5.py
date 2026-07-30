from pyspark.sql.functions import col

from src.Question5.util import (
    create_employee_df,
    create_department_df,
    create_country_df,
    average_salary,
    employee_start_m,
    add_bonus,
    reorder_columns,
    dynamic_join,
    replace_state_with_country,
    convert_lowercase,
    add_load_date
)


# ----------------------------------------
# Create DataFrames
# ----------------------------------------

employee_df = create_employee_df(spark)
department_df = create_department_df(spark)
country_df = create_country_df(spark)


# ----------------------------------------
# Test Employee DataFrame
# ----------------------------------------

assert employee_df.count() == 7

assert employee_df.columns == [
    "employee_id",
    "employee_name",
    "department",
    "State",
    "salary",
    "Age"
]

print("Employee DataFrame Test Passed")


# ----------------------------------------
# Test Department DataFrame
# ----------------------------------------

assert department_df.count() == 5

assert department_df.columns == [
    "dept_id",
    "dept_name"
]

print("Department DataFrame Test Passed")


# ----------------------------------------
# Test Country DataFrame
# ----------------------------------------

assert country_df.count() == 3

print("Country DataFrame Test Passed")


# ----------------------------------------
# Test Average Salary
# ----------------------------------------

avg_df = average_salary(employee_df)

salary = {
    row["department"]: row["avg_salary"]
    for row in avg_df.collect()
}

assert salary["D101"] == 8600
assert salary["D102"] == 8700
assert salary["D103"] == 8550

print("Average Salary Test Passed")


# ----------------------------------------
# Test Employee Name Starts With M
# ----------------------------------------

m_df = employee_start_m(
    employee_df,
    department_df
)

names = [
    row.employee_name
    for row in m_df.collect()
]

assert len(names) == 2
assert "michel" in names
assert "maria" in names

print("Employee Starts With M Test Passed")


# ----------------------------------------
# Test Bonus
# ----------------------------------------

bonus_df = add_bonus(employee_df)

assert "bonus" in bonus_df.columns

bonus = (
    bonus_df
    .filter(col("employee_id") == 11)
    .select("bonus")
    .first()[0]
)

assert bonus == 18000

print("Bonus Test Passed")


# ----------------------------------------
# Test Reorder Columns
# ----------------------------------------

reordered_df = reorder_columns(employee_df)

assert reordered_df.columns == [
    "employee_id",
    "employee_name",
    "salary",
    "State",
    "Age",
    "department"
]

print("Reorder Columns Test Passed")


# ----------------------------------------
# Test Inner Join
# ----------------------------------------

inner_df = dynamic_join(
    employee_df,
    department_df,
    "inner"
)

assert inner_df.count() == 7
assert "dept_name" in inner_df.columns

print("Inner Join Test Passed")


# ----------------------------------------
# Test Left Join
# ----------------------------------------

left_df = dynamic_join(
    employee_df,
    department_df,
    "left"
)

assert left_df.count() == 7

print("Left Join Test Passed")


# ----------------------------------------
# Test Right Join
# ----------------------------------------

right_df = dynamic_join(
    employee_df,
    department_df,
    "right"
)

assert right_df.count() == 9

print("Right Join Test Passed")


# ----------------------------------------
# Test Replace State
# ----------------------------------------

country_employee_df = replace_state_with_country(
    employee_df,
    country_df
)

state = (
    country_employee_df
    .filter(col("employee_id") == 11)
    .select("State")
    .first()[0]
)

assert state == "newyork"

print("Replace State Test Passed")


# ----------------------------------------
# Test Lowercase Columns
# ----------------------------------------

lower_df = convert_lowercase(
    country_employee_df
)

for column in lower_df.columns:
    assert column == column.lower()

print("Lowercase Column Test Passed")


# ----------------------------------------
# Test Load Date
# ----------------------------------------

final_df = add_load_date(
    lower_df
)

assert "load_date" in final_df.columns

print("Load Date Test Passed")


print("\n===================================")
print("All Question 5 Tests Passed")
print("===================================")