## Functions to allow direct access to main components of inventory processing scripts
## Currently just an initial outline

import pandas as pd
from stewi.globals import get_required_fields



outputpath = 'StandardizedReleaseandWasteInventories/output/'
formatpath = {'flowbyfacility':""}

def seeAvailbleInventoriesandYears ():
# reads a list of available inventories and prints them here, like:
# National Emissions Inventory: 2014
# Greenhouse Gas Reporting Program: 2015, 2016
# ....
    print("Test")

def getInventory(inventory_acronym,year,format='flowbyfacility',filter_for_LCI=False,US_States_Only=False):
#Returns an inventory file as a data frame
    path = outputpath+formatpath[format]
    file = path+inventory_acronym+'_'+str(year)+'.csv'
    required_fields = get_required_fields(format)
    cols = list(required_fields.keys())
    inventory = pd.read_csv(file,header=0,usecols=cols,dtype=required_fields)
    return inventory


