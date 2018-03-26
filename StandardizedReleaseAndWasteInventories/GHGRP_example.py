#GHG Reporting Program Data for LCI
#Retrieve data from Envirofacts Web Service in json format

import pandas as pd
import requests

#Basic API docs: https://www.epa.gov/enviro/envirofacts-data-service-api
baseurl = 'https://iaspub.epa.gov/enviro/efservice/'

#Set year
year = '2015'

#Select table from GHG RP
#Models with tables available at https://www.epa.gov/enviro/greenhouse-gas-model
table_id='AA_SUBPART_LEVEL_INFORMATION'

#Limit number of rows for testing
rowmax = '100'

#Form url. This should be replaced with dynamic queries
url = baseurl + table_id + '/REPORTING_YEAR/=/' + year + '/ROWS/0:' + rowmax + '/json'

#Return request object from query
subpartAArequest = requests.get(url)

#Render request as json
subpartAAjson = subpartAArequest.json()

#Save json as a dataframe
subpartAAdf = pd.DataFrame(subpartAAjson)
#Peek at dataframe
subpartAAdf.head(50)

