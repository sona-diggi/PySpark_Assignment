from src.Question4.driver import employee_df
from src.Question4.util import *

print("Question 1")

employee_df.show(truncate=False)

employee_df.printSchema()


print("Question 2")

flatten_df = flatten_dataframe(employee_df)

flatten_df.show(truncate=False)


print("Question 3")

record_count(flatten_df)


print("Question 4")

print("explode()")

explode_example(flatten_df)

print("explode_outer()")

explode_outer_example(flatten_df)

print("posexplode()")

posexplode_example(flatten_df)


print("Question 5")

filter_df = filter_employee(flatten_df)

filter_df.show(truncate=False)


print("Question 6")

snake_df = rename_columns(flatten_df)

print(snake_df.columns)


print("Question 7")

snake_df = add_load_date(snake_df)

snake_df.show(truncate=False)


print("Question 8")

snake_df = create_partition_columns(snake_df)

snake_df.show(truncate=False)


print("Question 9")

write_table(snake_df)