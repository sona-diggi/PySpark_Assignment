from src.Question4 import util
 
 
def main():
 
    # Read JSON file
    employee_df = util.read_json_dynamic(
        spark,
        "/Volumes/workspace/sona/s1/nested_json_file.json"
    )
 
    print("Original Data")
    display(employee_df)
 
    # Flatten DataFrame
    flat_df = util.flatten_df(employee_df)
 
    print("Flattened Data")
    display(flat_df)
 
    # Record Count
    print("Original Record Count :", util.record_count(employee_df))
    print("Flattened Record Count :", util.record_count(flat_df))
 
    # explode()
    print("explode()")
    display(util.explode_demo(employee_df))
 
    # explode_outer()
    print("explode_outer()")
    display(util.explode_outer_demo(employee_df))
 
    # posexplode()
    print("posexplode()")
    display(util.posexplode_demo(employee_df))
 
    # Filter id
    filtered_df = util.filter_id(flat_df)
 
    print("Filtered Data")
    display(filtered_df)
 
    # Rename columns
    renamed_df = util.rename_columns_snake(filtered_df)
 
    print("Snake Case Columns")
    display(renamed_df)
 
    # Add load date
    load_df = util.add_load_date(renamed_df)
 
    # Add year, month and day
    final_df = util.add_partition_columns(load_df)
 
    print("Final Data")
    display(final_df)
 
    # Create Database
    spark.sql("CREATE DATABASE IF NOT EXISTS employee")
 
    # Write table
    util.write_partitioned_table(final_df)
 
    print("Question 4 Completed Successfully")
 
 
if __name__ == "__main__":
    main()
 

 
