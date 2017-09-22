'''
STATUS:
using #json_data = requests.get(url).json()
First time the query ran a <response 200> was recieved indicating everything ran smoothly
However when trying to print the json for a check the following error was thrown:
{'Results': {'Error': {'ErrorMessage': 'Your search results returned 249,382 records. 
Maximum number of records must not exceed 100,000.'}}}
After removing the print statement the error did not occur again

Using json_data = pd.read_json(url)
Error test described above is located in the first column and row of the dataframe
KeyError: 'Results'
Need to adjust query to include fewer records
Will update code Sat Sept 22.
'''

import requests
import pandas as pd
import json


main_api = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_custom_data_'
service_parameter = 'facility?' #define which parameter is primary search criterion
address = 'p_year=2015' #define any secondary search criteria
output_type = 'JSON' #define output type

#creates a url from various search parameters
def create_url(main_api, service_parameter,address, output_type):
	url = main_api + service_parameter + address + '&output=' + output_type
	return url

url = create_url(main_api, service_parameter, address, output_type)

#queries the data based on the previously defined url and
#transforms it into a data frame
def json_pull_to_df(url):
	#json_data = requests.get(url).json()
	#df = pd.DataFrame(json_data)
	json_data = pd.read_json(url)
	df = pd.DataFrame(json_data['Results']['Results'])  
	return df

DMR_df = json_pull_to_df(url)
print(DMR_df.head())
