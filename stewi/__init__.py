## Functions to allow direct access to main components of inventory processing scripts

import pandas as pd
import os
import logging
from stewi.globals import get_required_fields, get_optional_fields, filter_inventory, filter_states,inventory_single_compartments

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'stewi/'

output_dir = modulepath + 'output/'
data_dir = modulepath + 'data/'
formatpath = {'flowbyfacility':"flowbyfacility/",'flow':"flow/",'facility':"facility/"}

def seeAvailableInventoriesandYears(format='flowbyfacility'):
# reads a list of available inventories and prints them here, like:
# NEI: 2014
# GHGRP: 2015, 2016
# ....
    files = os.listdir(output_dir+formatpath[format])
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
         acronym = file[:e_acronym]
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

#Only functions for flowbyfacility at this time
def getInventory(inventory_acronym, year, format='flowbyfacility', filter_for_LCI=False, US_States_Only=False):
    # Returns an inventory file as a data frame
    path = output_dir+formatpath[format]
    file = path+inventory_acronym+'_'+str(year)+'.csv'
    fields = get_required_fields(format)
    inventory = pd.read_csv(file, header=0, dtype=fields)
    #Add in units and compartment if not present
    if 'Unit' not in inventory.columns:
        inventory['Unit'] = 'kg'
    if 'Compartment' not in inventory.columns:
        inventory['Compartment'] = inventory_single_compartments[inventory_acronym]
    #Apply filters if present
    if US_States_Only: inventory = filter_states(inventory)
    if filter_for_LCI:
        filter_path = data_dir
        if inventory_acronym == 'TRI':
            filter_path += 'TRI_pollutant_omit_list.csv'
            filter_type = 'drop'
            inventory = filter_inventory(inventory, filter_path, filter_type=filter_type)
        elif inventory_acronym == 'GHGRP':
            filter_path += 'ghg_mapping.csv'
            filter_type = 'keep'
            inventory = filter_inventory(inventory, filter_path, filter_type=filter_type)
        elif inventory_acronym == 'RCRAInfo': filter_type = ''
        elif inventory_acronym == 'eGRID': filter_type = ''
        elif inventory_acronym == 'NEI':
            filter_path += 'NEI_pollutant_omit_list.csv'
            filter_type = 'drop'
            inventory = filter_inventory(inventory, filter_path, filter_type=filter_type)
    return inventory


def getInventoryFlows(inventory_acronym, year):
    path = output_dir + formatpath['flow']
    file = path + inventory_acronym + '_' + str(year) + '.csv'
    flows = pd.read_csv(file, header=0)
    return flows

def getInventoryFacilities(inventory_acronym, year):
    path = output_dir + formatpath['facility']
    file = path + inventory_acronym + '_' + str(year) + '.csv'
    facilities = pd.read_csv(file, header=0, dtype={"FacilityID":"str"})
    return facilities