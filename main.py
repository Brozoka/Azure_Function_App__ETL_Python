import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func
from io import StringIO
import yaml

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    # get the credentials from a YAML file
    credentials = yaml.load(open('./credentials.yml'), Loader=yaml.FullLoader)
    
    #connect to ADLS
    STORAGEACCOUNTURL= credentials['STORAGEACCOUNTURL']
    STORAGEACCOUNTKEY= credentials['STORAGEACCOUNTKEY']
    LOCALFILENAME= ['file_name.csv', 'file_name.csv']
    
    #create 2 dataframes
    file1 = pd.DataFrame()
    file2 = pd.DataFrame()
    
    #download the data from ADLS
    service_client = DataLakeServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    adl_client_instance = service_client.get_file_system_client(file_system="input")
    directory_client = adl_client_instance.get_directory_client("input")
    
    #load the data into dataframes
    file_client1 = adl_client_instance.get_file_client(LOCALFILENAME[0])
    adl_data1 = file_client1.download_file()
    byte1 = adl_data1.readall()
    s=str(byte1,'utf-8')
    file1 = pd.read_csv(StringIO(s))
    file_client2 = adl_client_instance.get_file_client(LOCALFILENAME[1])
    adl_data2 = file_client2.download_file()
    byte2 = adl_data2.readall()
    s=str(byte2,'utf-8')
    file2 = pd.read_csv(StringIO(s))
    
    # replace all null values with either 0 or a string,  lso do type casting in case we need a type other than string
    file1['column name'] = file1['column name'].fillna(0)
    file1['column name'] = file1['column name'].fillna(int(0))
    file1['column name'] = file1['column name'].astype(str).fillna('String to replace null values')
   
    # replace all null values with either 0 or a string, also do type casting in case we need a type other than string
    file2['column name'] = file2['column name'].fillna(0)
    file2['column name'] = file2['column name'].fillna(int(0))
    file2['column name'] = file2['column name'].astype(str).fillna('String to replace null values')

    # optional : renaming the column to avoid duplicate columns names
    file1 = file1.rename(columns={'column to be changed': 'change it to this string'})
    
    # merge the 2 csv files
    output = pd.merge(left=file1, right=file2, on='key column name', how='inner')
    
    #upload the file to adls
    service_client = DataLakeServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    file_system_client = service_client.get_file_system_client(file_system="output")
    directory_client = file_system_client.get_directory_client("output") 
    file_client = directory_client.create_file("output.parquet") 
    file_contents = summary.to_parquet()
    file_client.append_data(data=file_contents, offset=0, length=len(file_contents)) 
    
    file_client.flush_data(len(file_contents))

    return("This HTTP triggered function executed successfully.")
