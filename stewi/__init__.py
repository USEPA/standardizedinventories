## Functions to allow direct access to main components of inventory processing scripts
## Currently just an initial outline

import pandas as pd
import os
import logging
from stewi.globals import get_required_fields, get_optional_fields, filter_inventory, filter_states

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

#for testing
#modulepath = 'stewi/'

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'stewi/'

output_dir = modulepath + 'output/'
data_dir = modulepath + 'data/'
formatpath = {'flowbyfacility':"",'flow':"flow/",'facility':"facility/"}


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
def getInventory(inventory_acronym, year, format='flowbyfacility', include_optional_fields=True,
                 filter_for_LCI=False, US_States_Only=False):
    # Returns an inventory file as a data frame
    path = output_dir+formatpath[format]
    file = path+inventory_acronym+'_'+str(year)+'.csv'
    fields = get_required_fields(format)
    if include_optional_fields:
        optional_fields_all_inventories =  get_optional_fields(format)
        #check if inventory has optional fields
        fields_file = data_dir+format+'_optional_output_fields.json'
        outputoptionalfieldsdict = pd.read_json(fields_file, typ='dict')
        if outputoptionalfieldsdict[inventory_acronym] is not "NA":
            optional_fields_present = outputoptionalfieldsdict[inventory_acronym]
            log.debug('optional_fields_present: '+ str(optional_fields_present))
            for v in optional_fields_all_inventories.keys():
                if v in optional_fields_present:
                    fields[v] = optional_fields_all_inventories[v]
    cols = list(fields.keys())
    inventory = pd.read_csv(file, header=0, usecols=cols, dtype=fields)
    if US_States_Only: inventory = filter_states(inventory)
    if filter_for_LCI:
        inventory_acronym = inventory_acronym.lower()
        filter_path = data_dir
        if inventory_acronym == 'tri':
            filter_path += 'TRI_pollutant_omit_list.csv'
            filter_type = 'drop'
        elif inventory_acronym == 'GHGRP':
            filter_path += 'ghg_mapping.csv'
            filter_type = 'keep'
        elif inventory_acronym == 'dmr':
            filter_path += 'DMR_Pollutant_ListwithExclusionsforLCI.xlsx'
            filter_type = 'mark_drop'
        elif inventory_acronym == 'rcrainfo': filter_type = ''
        elif inventory_acronym == 'egrid': filter_type = ''
        elif inventory_acronym == 'nei': filter_type = ''
        if (not os.path.exists(filter_path)) or filter_path == data_dir:
            print('No filter criteria file for this source in data directory')
        else: inventory = filter_inventory(inventory, filter_path, filter_type=filter_type)
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