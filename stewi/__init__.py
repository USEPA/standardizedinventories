"""
Functions to return inventory data for a single inventory in standard formats
"""

import os
import pandas as pd
from stewi.globals import get_required_fields, get_optional_fields, filter_inventory,\
    filter_states, inventory_single_compartments

try:
    MODULEPATH = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError:
    MODULEPATH = 'stewi/'

OUTPUT_DIR = MODULEPATH + 'output/'
DATA_DIR = MODULEPATH + 'data/'
FORMATPATH = {'flowbyfacility': "flowbyfacility/", 'flow': "flow/", 'facility': "facility/"}


def seeAvailableInventoriesandYears(stewiformat='flowbyfacility'):
    """Prints available inventories and years for a given output format
    :param stewiformat: 'flowbyfacility' only current option
    :return: prints like
    NEI: 2014
    TRI: 2015, 2016
    """
    files = os.listdir(OUTPUT_DIR + FORMATPATH[stewiformat])
    outputfiles = []
    existing_inventories = {}
    for name in files:
        if name.endswith(".csv"):
            _n = name.strip('.csv')
            outputfiles.append(_n)
        elif name.endswith(".parquet"):
            _n = name.strip('.parquet')
            outputfiles.append(_n)
    for file in outputfiles:
        length = len(file)
        s_yr = length - 4
        e_acronym = length - 5
        year = file[s_yr:]
        acronym = file[:e_acronym]
        if acronym not in existing_inventories.keys():
            existing_inventories[acronym] = [year]
        else:
            existing_inventories[acronym].append(year)
    print(stewiformat + ' inventories available (name, year):')
    for i in existing_inventories.keys():
        _s = i + ": "
        for _y in existing_inventories[i]:
            _s = _s + _y + ","
        print(_s)


def getInventory(inventory_acronym, year, stewiformat='flowbyfacility', filter_for_LCI=False,
                 US_States_Only=False):
    """Returns an inventory in a standard output format
    :param inventory_acronym: like 'TRI'
    :param year: year as number like 2010
    :param stewiformat: standard output format for returning..'flowbyfacility' is only current
    :param filter_for_LCI: whether or not to filter inventory for life cycle inventory creation
    :param US_States_Only: includes only US states
    :return: dataframe with standard fields depending on output format
    """
    path = OUTPUT_DIR + FORMATPATH[stewiformat]
    file = path + inventory_acronym + '_' + str(year) + '.csv'
    fields = get_required_fields(stewiformat)
    if os.path.exists(file):
        inventory = pd.read_csv(file, header=0, dtype=fields)
    else:
        file = file[:-3]+'parquet'
        inventory = pd.read_parquet(file)
    # Add in units and compartment if not present
    if 'Unit' not in inventory.columns:
        inventory['Unit'] = 'kg'
    if 'Compartment' not in inventory.columns:
        inventory['Compartment'] = inventory_single_compartments[inventory_acronym]
    # Apply filters if present
    if US_States_Only:
        inventory = filter_states(inventory)
    if filter_for_LCI:
        filter_path = DATA_DIR
        if inventory_acronym == 'TRI':
            filter_path += 'TRI_pollutant_omit_list.csv'
            filter_type = 'drop'
            inventory = filter_inventory(inventory, filter_path, filter_type=filter_type)
        elif inventory_acronym == 'GHGRP':
            filter_path += 'ghg_mapping.csv'
            filter_type = 'keep'
            inventory = filter_inventory(inventory, filter_path, filter_type=filter_type)
        elif inventory_acronym == 'RCRAInfo':
            filter_type = ''
        elif inventory_acronym == 'eGRID':
            filter_type = ''
        elif inventory_acronym == 'NEI':
            filter_path += 'NEI_pollutant_omit_list.csv'
            filter_type = 'drop'
            inventory = filter_inventory(inventory, filter_path, filter_type=filter_type)
    return inventory


def getInventoryFlows(inventory_acronym, year):
    """Returns flows for an inventory
    :param inventory_acronym: e.g. 'TRI'
    :param year: e.g. 2014
    :return: dataframe with standard flows format
    """
    path = OUTPUT_DIR + FORMATPATH['flow']
    file = path + inventory_acronym + '_' + str(year) + '.csv'
    flows = pd.read_csv(file, header=0)
    return flows


def getInventoryFacilities(inventory_acronym, year):
    """Returns flows for an inventory
    :param inventory_acronym: e.g. 'TRI'
    :param year: e.g. 2014
    :return: dataframe with standard flows format
    """
    path = OUTPUT_DIR + FORMATPATH['facility']
    file = path + inventory_acronym + '_' + str(year) + '.csv'
    facilities = pd.read_csv(file, header=0, dtype={"FacilityID": "str"})
    return facilities
