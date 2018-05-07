## Functions to allow direct access to main components of inventory processing scripts
## Currently just an initial outline

import pandas as pd
import os
from stewi.globals import get_required_fields

#for testing
#modulepath = 'stewi'

modulepath = os.path.dirname(__file__)

output_dir = modulepath+'/output/'
data_dir = modulepath+'/data/'
formatpath = {'flowbyfacility':""}

def seeAvailableInventoriesandYears(format='flowbyfacility'):
# reads a list of available inventories and prints them here, like:
# NEI: 2014
# GHGRP: 2015, 2016
# ....
    files = os.listdir(output_dir)
    outputfiles = []
    existing_inventories = {}
    for name in files:
        if name.endswith(".csv"):
            n = name.strip('.csv')
            outputfiles.append(n)
    for file in outputfiles:
         length = len(file)
         s_yr = length-4
         e_acronym = length-5
         year = file[s_yr:]
         acronym = str.upper(file[:e_acronym])
         if (acronym not in existing_inventories.keys()):
             existing_inventories[acronym] = [year]
         else:
             existing_inventories[acronym].append(year)
    print (format + ' inventories available (name, year):')
    for i in existing_inventories.keys():
        s = i + ": "
        for y in existing_inventories[i]:
            s = s + y + ","
        print(s)

def getInventory(inventory_acronym,year,format='flowbyfacility',filter_for_LCI=False,US_States_Only=False):
#Returns an inventory file as a data frame
    path = output_dir+formatpath[format]
    file = path+inventory_acronym+'_'+str(year)+'.csv'
    required_fields = get_required_fields(format)
    cols = list(required_fields.keys())
    inventory = pd.read_csv(file,header=0,usecols=cols,dtype=required_fields)
    return inventory


