from src.Question4.util import (
    read_json_dynamic,
    flatten_df,
    record_count,
    filter_id,
    rename_columns_snake,
    add_load_date,
    add_partition_columns,
    camel_to_snake
)
 
 
# -----------------------------------
# Read JSON Test
# -----------------------------------
 
employee_df = read_json_dynamic(
    spark,
    "/Volumes/workspace/sona/s1/nested_json_file.json"
)
 
assert record_count(employee_df) == 1
 
print("Read JSON test passed")
 
 
# -----------------------------------
# Flatten JSON Test
# -----------------------------------
 
flat_df = flatten_df(employee_df)
 
assert record_count(flat_df) == 3
 
print("Flatten test passed")
 
 
# -----------------------------------
# Filter Test
# -----------------------------------
 
filtered_df = filter_id(flat_df)
 
assert filtered_df.count() == 3
 
print("Filter test passed")
 
 
# -----------------------------------
# Camel Case Conversion Test
# -----------------------------------
 
assert camel_to_snake("employeeName") == "employee_name"
 
assert camel_to_snake("companyName") == "company_name"
 
assert camel_to_snake("storeSize") == "store_size"
 
print("Camel case conversion test passed")
 
 
# -----------------------------------
# Rename Column Test
# -----------------------------------
 
rename_df = rename_columns_snake(flat_df)
 
 
assert "company_name" in rename_df.columns
 
assert "store_size" in rename_df.columns
 
assert "employee_id" in rename_df.columns
 
assert "employee_name" in rename_df.columns
 
 
print("Rename columns test passed")
 
 
# -----------------------------------
# Load Date Test
# -----------------------------------
 
load_df = add_load_date(rename_df)
 
 
assert "load_date" in load_df.columns
 
 
print("Load date test passed")
 
 
# -----------------------------------
# Partition Column Test
# -----------------------------------
 
partition_df = add_partition_columns(load_df)
 
 
assert "year" in partition_df.columns
 
assert "month" in partition_df.columns
 
assert "day" in partition_df.columns
 
 
print("Partition columns test passed")
 
 
# -----------------------------------
# Final Result
# -----------------------------------
 
print("All Question 4 test cases passed successfully")