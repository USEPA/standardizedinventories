#Status: pulls json down by all facilities with a year = 2015 by SIC code = 99
#saves json data to output directory specified
#Next steps: iterate through SIC list and combine query output to a single json file

import requests
import pandas as pd
import json
import os

main_api = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_custom_data_'
service_parameter = 'facility?' #define which parameter is primary search criterion
year = 'p_year=2015' #define year
form_obj = '&p_sic2=99' #define any secondary search criteria
output_type = 'JSON' #define output type

def set_output_dir(directory):
    outputdir = directory 
    if not os.path.exists(outputdir): os.makedirs(outputdir)
    return outputdir

outputdir = set_output_dir('../LCI-Primer-Output/')


#creates a url from various search parameters
def create_url_and_get(main_api, service_parameter,year, form_obj, output_type):
	url = main_api + service_parameter + year + form_obj+ '&output=' + output_type
	result = requests.get(url).json()
	return result

json_data = create_url_and_get(main_api, service_parameter, year, form_obj, output_type)
print(json_data)


def write_json_file(path, file, data):
    final_path = path + file + '.json'
    with open(final_path, 'w') as fp:
        json.dump(data, fp)

x = write_json_file(outputdir, 'DMR_data', json_data)