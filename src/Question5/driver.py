# ----------------------------------------
# Import utility functions
# ----------------------------------------

from util import (
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
    add_load_date,
    write_external_tables
)



# ----------------------------------------
# Create DataFrames
# ----------------------------------------

employee_df = create_employee_df(spark)

department_df = create_department_df(spark)

country_df = create_country_df(spark)



print("Employee DataFrame")
display(employee_df)



print("Department DataFrame")
display(department_df)



print("Country DataFrame")
display(country_df)



# ----------------------------------------
# 2. Average Salary Department Wise
# ----------------------------------------

avg_salary_df = average_salary(employee_df)

print("Average Salary Department Wise")

display(avg_salary_df)



# ----------------------------------------
# 3. Employee Name Starts With m
# ----------------------------------------

employee_m_df = employee_start_m(
    employee_df,
    department_df
)


print("Employee Name Starts With m")

display(employee_m_df)



# ----------------------------------------
# 4. Add Bonus Column
# ----------------------------------------

employee_bonus_df = add_bonus(employee_df)


print("Employee DataFrame With Bonus")

display(employee_bonus_df)



# ----------------------------------------
# 5. Reorder Columns
# ----------------------------------------

reordered_employee_df = reorder_columns(employee_df)


print("Reordered Employee DataFrame")

display(reordered_employee_df)



# ----------------------------------------
# 6. Inner Join
# ----------------------------------------

inner_join_df = dynamic_join(
    employee_df,
    department_df,
    "inner"
)


print("Inner Join Result")

display(inner_join_df)



# ----------------------------------------
# Left Join
# ----------------------------------------

left_join_df = dynamic_join(
    employee_df,
    department_df,
    "left"
)


print("Left Join Result")

display(left_join_df)



# ----------------------------------------
# Right Join
# ----------------------------------------

right_join_df = dynamic_join(
    employee_df,
    department_df,
    "right"
)


print("Right Join Result")

display(right_join_df)



# ----------------------------------------
# 7. Replace State with Country Name
# ----------------------------------------

country_employee_df = replace_state_with_country(
    employee_df,
    country_df
)


print("Employee DataFrame With Country Name")

display(country_employee_df)



# ----------------------------------------
# 8. Lowercase Columns + Load Date
# ----------------------------------------

lowercase_df = convert_lowercase(
    country_employee_df
)


final_employee_df = add_load_date(
    lowercase_df
)


print("Final DataFrame With Lowercase Columns and Load Date")

display(final_employee_df)



# ----------------------------------------
# 9. Create External Parquet and CSV Files
# ----------------------------------------

write_external_tables(
    final_employee_df
)


print("External Parquet and CSV tables created successfully")