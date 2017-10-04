#!/usr/bin/env python

#results column is still nested. json_normalize does not fix this issue. Will try entering 
#specifc column names.
#df = pd.DataFrame["Results"]["Results"] throws a type error.
import json
import pandas as pd 
from pprint import pprint
from pandas.io.json import json_normalize


path = '../LCI-Primer-Output/DMR_data.json' #path to json data output from DMR query

#loads json data from DMR query
def import_json(path):
	with open(path, 'r') as f:
		data = json.load(f)
		return data

json_data = import_json(path)


data = json_normalize(json_data) 
df = pd.DataFrame(data)
df.head()

#if __name__ == '__main__':
 #   main()