# __init__.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Public API for stewi. Functions to return inventory data for a single
inventory in standard formats
"""


import os
from stewi.globals import get_required_fields,\
    log, add_missing_fields, output_dir,\
    WRITE_FORMAT, read_inventory, stewi_formats, paths,\
    read_source_metadata, inventory_formats, set_stewi_meta
from stewi.filter import apply_filter_to_inventory


def getAvailableInventoriesandYears(stewiformat='flowbyfacility'):
    """Gets available inventories and years for a given output format
    :param stewiformat: e.g. 'flowbyfacility'
    :return: existing_inventories dictionary of inventories like:
        {NEI: [2014],
         TRI: [2015, 2016]}
    """
    existing_inventories = {}
    if stewiformat not in stewi_formats:
        log.error('not a supported stewi format')
        return existing_inventories
    directory = output_dir + '/' + stewiformat + '/'
    if os.path.exists(directory):
        files = os.listdir(directory)
    else:
        log.error('directory not found: ' + directory)
        return existing_inventories
    outputfiles = []
    for name in files:
        if name.endswith(WRITE_FORMAT):
            _n = name[:-len('.'+WRITE_FORMAT)]
            if '_v' in _n:
                _n = _n[:_n.find('_v')]
            outputfiles.append(_n)
    # remove duplicates
    outputfiles = list(set(outputfiles))
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
    for key in existing_inventories.keys():
        existing_inventories[key].sort()
    return existing_inventories


def seeAvailableInventoriesandYears(stewiformat='flowbyfacility'):
    """Gets available inventories and years for a given output format
    :param stewiformat: e.g. 'flowbyfacility' or 'flow' """
    existing_inventories = getAvailableInventoriesandYears(stewiformat)
    if existing_inventories == {}:
        print('No inventories found')
    else:
        print(stewiformat + ' inventories available (name, year):')
        for i in existing_inventories.keys():
            _s = i + ": "
            for _y in existing_inventories[i]:
                _s = _s + _y + ", "
            _s = _s[:-2]
            print(_s)


def getInventory(inventory_acronym, year, stewiformat='flowbyfacility', 
                 filter_list=[], filter_for_LCI=False, US_States_Only=False):
    """Returns an inventory in a standard output format
    :param inventory_acronym: like 'TRI'
    :param year: year as number like 2010
    :param stewiformat: standard output format for returning..'flowbyfacility'
        or 'flowbyprocess' only 
    :param filter_list: a list of named filters to apply to inventory
    :param filter_for_LCI: whether or not to filter inventory for life
        cycle inventory creation
    :param US_States_Only: includes only US states
    :return: dataframe with standard fields depending on output format
    """
    if stewiformat not in inventory_formats:
        log.error('%s is not a supported format for getInventory',
                  stewiformat)
        return None
    inventory = read_inventory(inventory_acronym, year, stewiformat)
    if inventory is None:
        return None
    fields = get_required_fields(stewiformat)
    fields = {key: value for key, value in fields.items() if key in list(inventory)}
    inventory = inventory.astype(fields)
    inventory = add_missing_fields(inventory, inventory_acronym, stewiformat)
    
    # for backwards compatability, maintain these optional parameters in getInventory
    if filter_for_LCI:
        if 'filter_for_LCI' not in filter_list:
            filter_list.append('filter_for_LCI')
    if US_States_Only:
        if 'US_States_only' not in filter_list:
            filter_list.append('US_States_only')
            
    if filter_list != []:
        inventory = apply_filter_to_inventory(inventory, inventory_acronym, 
                                              filter_list)
    return inventory


def getInventoryFlows(inventory_acronym, year):
    """Returns flows for an inventory
    :param inventory_acronym: e.g. 'TRI'
    :param year: e.g. 2014
    :return: dataframe with standard flows format
    """
    flows = read_inventory(inventory_acronym, year, 'flow')
    return flows


def getInventoryFacilities(inventory_acronym, year):
    """Returns flows for an inventory
    :param inventory_acronym: e.g. 'TRI'
    :param year: e.g. 2014
    :return: dataframe with standard flows format
    """
    facilities = read_inventory(inventory_acronym, year, 'facility')
    return facilities

def getMetadata(inventory_acroynym, year):
    """Returns metadata in the form of a dictionary as read from stored JSON file
    :param inventory_acronym: e.g. 'TRI'
    :param year: e.g. 2014
    :return: metadata dictionary
    """
    meta = read_source_metadata(paths,
                                set_stewi_meta(inventory_acroynym + '_' + str(year)),
                                force_JSON=True)
    return meta
