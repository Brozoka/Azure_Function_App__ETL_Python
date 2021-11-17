import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func
from io import StringIO

def main(req: func.HttpRequest) -> func.HttpResponse:
    STORAGEACCOUNTURL= 'https://{storage account name}.blob.core.windows.net/'
    STORAGEACCOUNTKEY= '****'
    LOCALFILENAME= ['file1.csv', 'file2.csv']
    CONTAINERNAME= 'name of the container'

    file1 = pd.DataFrame()
    file2 = pd.DataFrame()
    #download from blob

    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    for i in LOCALFILENAME:
            blob_client_instance = blob_service_client_instance.get_blob_client(container=CONTAINERNAME, blob=i, snapshot=None)
            blob_data = blob_client_instance.download_blob()
            if i == 'file1.csv':
                file1 = pd.read_csv(StringIO(blob_data.content_as_text()))
            if i == 'file2.csv':
                file2 = pd.read_csv(StringIO(blob_data.content_as_text()))
    
    # replace all null values with either 0 or a string, also do type casting for strings
    file1['column name'] = file1['column name'].fillna(0)

    # replace all null values with either 0 or a string, also do type casting for strings
    file2['column name'] = file2['column name'].fillna(int(0))
    
    # load
    summary = pd.merge(left=file1, right=file2, on='key column name', how='inner')
    summary.to_csv()

    service_client = DataLakeServiceClient(account_url="https://{storage account name}.dfs.core.windows.net/", credential=STORAGEACCOUNTKEY)
    file_system_client = service_client.get_file_system_client(file_system="{container name}")
    directory_client = file_system_client.get_directory_client("{folder name}") 
    file_client = directory_client.create_file("{file name}.csv") 
    file_contents = summary.to_csv()
    file_client.upload_data(file_contents, overwrite=True) 

    return("This HTTP triggered function executed successfully.")

if __name__ == '__main__':
    main("name")
