import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func
from io import StringIO
import yaml

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    credentials = yaml.load(open('./credentials.yml'), Loader=yaml.FullLoader)
    
    STORAGEACCOUNTURL= credentials['STORAGEACCOUNTURL']
    STORAGEACCOUNTKEY= credentials['STORAGEACCOUNTKEY']
    LOCALFILENAME= ['beers.csv', 'breweries.csv']

    beer = pd.DataFrame()
    breweries = pd.DataFrame()
    
    #download the data from adls
    service_client = DataLakeServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    adl_client_instance = service_client.get_file_system_client(file_system="raw")
    directory_client = adl_client_instance.get_directory_client("raw")

    #load the data into dataframes
    file_client = adl_client_instance.get_file_client(LOCALFILENAME[0])
    adl_data = file_client.download_file()
    beerbyte = adl_data.readall()
    s=str(beerbyte,'utf-8')
    beer = pd.read_csv(StringIO(s))
            
    file_client = adl_client_instance.get_file_client(LOCALFILENAME[1])
    adl_data = file_client.download_file()
    brewriesbyte = adl_data.readall()
    s=str(brewriesbyte,'utf-8')
    breweries = pd.read_csv(StringIO(s))
    
    # replace all null values with either 0 or a string, also do type casting for strings
    beer['abv'] = beer['abv'].fillna(0)
    beer['ibu'] = beer['ibu'].fillna(0)
    beer['id'] = beer['id'].fillna(int(0))
    beer['name_beer'] = beer['name'].astype(str).fillna('No name')
    beer['style'] = beer['style'].astype(str).fillna('No style')
    beer['brewery_id'] = beer['brewery_id'].fillna(int(0))
    beer['ounces'] = beer['ounces'].fillna(0)

    # replace all null values with either 0 or a string, also do type casting for strings
    breweries['brewery_id'] = breweries['brewery_id'].fillna(int(0))
    breweries['name'] = breweries['name'].astype(str).fillna('No name')
    breweries['city'] = breweries['city'].astype(str).fillna('No city')
    breweries['state'] = breweries['state'].astype(str).fillna('No state')

    # renaming the column to avoid duplicate columns names
    breweries = breweries.rename(columns={'name': 'name_breweries'})
    
    # merge the 2 csv files
    summary = pd.merge(left=beer, right=breweries, on='brewery_id', how='inner')
    
    #upload the file to adls
    service_client = DataLakeServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    file_system_client = service_client.get_file_system_client(file_system="output")
    directory_client = file_system_client.get_directory_client("output") 
    file_client = directory_client.create_file("output.parquet") 
    file_contents = summary.to_parquet()
    file_client.append_data(data=file_contents, offset=0, length=len(file_contents)) 
    
    file_client.flush_data(len(file_contents))

    return("This HTTP triggered function executed successfully.")

if __name__ == '__main__':
    main("name")
