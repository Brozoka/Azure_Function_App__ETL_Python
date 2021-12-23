Python script to transfer and transform data inside Azure ADLS storage using Visual Studio Code
-----------------
REQUIRED RESOURCES
-----------------
On PC:
-VS Code with a functionapp extension installed(you need to login to create a functionapp in VS Code)
-Python newest version
Inside Azure(Azure account):
-ADLS Storage(gen 2)
-FunctionApp(basic plan)
--------
THE CODE
--------
-The 'main.py' script moves 2 CSV files from an ADLS storage conatiner, merges them and moves it to a different container inside ADLS
-The 2 CSV files are merged together into one Parquet file based on a key column. Null values are replaced and type casting is also implemented.
-----------
ETL PROCESS
-----------
ADLS/input -> ADLS/output/output
'file1.csv' & 'file2.csv' -> 'output.parquet'
