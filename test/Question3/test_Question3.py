from src.Question3.driver import login_df
from src.Question3.util import *

print("Question 1")
login_df.show(truncate=False)

print("Question 2")
login_df = rename_columns(login_df)
login_df.show(truncate=False)

print("Question 3")
login_df = convert_to_timestamp(login_df)

result = user_actions_last_7_days(login_df)
result.show(truncate=False)

print("Question 4")
login_df = create_login_date(login_df)
login_df.show(truncate=False)

print("Question 5")
write_csv(login_df)
print("CSV file created successfully.")

print("Question 6")
write_managed_table(login_df)
print("Managed table created successfully.")