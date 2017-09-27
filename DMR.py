#Status: pulls json down by all facilities with a year = 2015 by SIC code = 99
#saves json data to output directory specified
#Next steps: iterate through SIC list and query based on changes to SIC parameter

import requests
import pandas as pd
import json
import os

DMR_year = '2015'
main_api = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_custom_data_'
service_parameter = 'facility?' #define which parameter is primary search criterion
year = 'p_year=' + DMR_year #define year
form_obj = '&p_sic2=' #define any secondary search criteria
output_type = 'JSON' #define output type

sic = ['01','02','07','08','09','10','12','13','14','15',\
'16','17','20','21','22','23','24','25','26','27','28','29'\
,'30','31','32','33','34','35','36','37','38','39','40','41',\
'42','43','44','45','46','47','48','49','50','51','52','53',\
'54','55','56','57','58','59','60','61','62','63','64','65'\
,'67','70','72','73','75','76','78','79','80','81','82'\
,'83','84','86','87','88','89','91','92','93','95','96','97','99']

def app_sic(form_obj, SIC):
	result = [form_obj + s for s in SIC]
	return result

sic_code_query = app_sic(form_obj, sic)
print(sic_code_query)

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

def query(): #under construction
	for i in urls:
		request.get(i).json()
		write_json_file(outputdir, 'data', json_data)

