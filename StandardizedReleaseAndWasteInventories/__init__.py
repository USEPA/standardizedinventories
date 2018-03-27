## Functions to allow direct access to main components of inventory processing scripts
## Currently just an initial outline

import pandas as pd

#reads a list of available inventories and prints them here, like:
#National Emissions Inventory: 2014
#Greenhouse Gas Reporting Program: 2015, 2016
#....
def seeAvailbleInventoriesandYears ():
    print("Test")

def getDMR(year,filter_for_LCI,US_States_Only):
    print("test")

def getTRI(year,filter_for_LCI,US_States_Only):
    print("Test getTRI")
    TRI = pd.read_csv('StandardizedReleaseandWasteInventories/output/TRI_'+ year + '.csv')
    return TRI
    #TRI.

#def getGHGRP

#def getRCRAInfo

#def getNEI