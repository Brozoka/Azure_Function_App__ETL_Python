# import the Pandas library
import pandas as pd

# extract

# load the csv data into the 2 dataframes
table_original = pd.read_csv(r'file_path', encoding='utf-8')
table_original_2 = pd.read_csv(r'file_path', encoding='utf-8')

# creating an empty dataframe for the final data
table = pd.DataFrame(columns=[''])

# transform

# replace all null values with either 0 or a string, also do type casting for strings
table['column_name'] = table_original['column_name'].fillna(0)

# replace all null values with either 0 or a string, also do type casting for strings
table_original_2['column_name'] = table_original_2['column_name'].fillna(int(0))

# load

# join the 2 dataframes into the final dataframe on the key column
summary = pd.merge(left=table, right=table_original_2, on='key_column_name', how='inner')

# load the data into a csv file and save it to a local folder
summary.to_csv(path_or_buf=r'save_path', index=False, encoding='utf-8')