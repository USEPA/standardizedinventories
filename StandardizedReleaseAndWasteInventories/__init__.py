## Functions to allow direct access to main components of inventory processing scripts
## Currently just an initial outline

import pandas as pd

#reads a list of available inventories and prints them here, like:
#National Emissions Inventory: 2014
#Greenhouse Gas Reporting Program: 2015, 2016
#....

outputpath = 'StandardizedReleaseandWasteInventories/output/'
formatpath = {'flowbyfacility':""}

def seeAvailbleInventoriesandYears ():
    print("Test")

def getInventory(inventory_acronym,year,format='flowbyfacility',filter_for_LCI=False,US_States_Only=False):
    path = outputpath+formatpath[format]
    file = path+inventory_acronym+'_'+str(year)+'.csv'
    inventory = pd.read_csv(file,header=0)
    return inventory

