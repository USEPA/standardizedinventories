# __init__.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Public API for stewi. Functions to return inventory data for a single
inventory in standard formats
"""


import os
from stewi.globals import get_required_fields, filter_inventory,\
    log, filter_states, add_missing_fields, output_dir, data_dir,\
    WRITE_FORMAT, read_inventory, stewi_formats, paths,\
    read_source_metadata, inventory_formats, set_stewi_meta


def getAvailableInventoriesandYears(stewiformat='flowbyfacility'):
    """Gets available inventories and years for a given output format
    :param stewiformat: e.g. 'flowbyfacility'
    :return: existing_inventories dictionary of inventories like:
        {NEI: [2014],
         TRI: [2015, 2016]}
    """
    if stewiformat not in stewi_formats:
        log.error('not a supported stewi format')
        return
    directory = output_dir + '/' + stewiformat + '/'
    if os.path.exists(directory):
        files = os.listdir(directory)
    else:
        log.error('directory not found: ' + directory)
        return
    outputfiles = []
    existing_inventories = {}
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
    print(stewiformat + ' inventories available (name, year):')
    for i in existing_inventories.keys():
        _s = i + ": "
        for _y in existing_inventories[i]:
            _s = _s + _y + ", "
        _s = _s[:-2]
        print(_s)


def getInventory(inventory_acronym, year, stewiformat='flowbyfacility', 
                 filter_for_LCI=False, US_States_Only=False):
    """Returns an inventory in a standard output format
    :param inventory_acronym: like 'TRI'
    :param year: year as number like 2010
    :param stewiformat: standard output format for returning..'flowbyfacility'
        or 'flowbyprocess' only 
    :param filter_for_LCI: whether or not to filter inventory for life
        cycle inventory creation
    :param US_States_Only: includes only US states
    :return: dataframe with standard fields depending on output format
    """
    if stewiformat not in inventory_formats:
        log.error('%s is not a supported format for getInventory',
                  stewiformat)
        return None
    fields = get_required_fields(stewiformat)
    inventory = read_inventory(inventory_acronym + '_' + str(year), stewiformat)
    if inventory is None:
        return None
    fields = {key: value for key, value in fields.items() if key in list(inventory)}
    inventory = inventory.astype(fields)
    inventory = add_missing_fields(inventory, inventory_acronym, stewiformat)
    # Apply filters if present
    if US_States_Only:
        inventory = filter_states(inventory)
    if filter_for_LCI:
        filter_path = data_dir
        filter_type = None
        if inventory_acronym == 'TRI':
            filter_path += 'TRI_pollutant_omit_list.csv'
            filter_type = 'drop'
        elif inventory_acronym == 'DMR':
            from stewi.DMR import remove_duplicate_organic_enrichment
            inventory = remove_duplicate_organic_enrichment(inventory)
            filter_path += 'DMR_pollutant_omit_list.csv'
            filter_type = 'drop'
        elif inventory_acronym == 'GHGRP':
            filter_path += 'ghg_mapping.csv'
            filter_type = 'keep'
        elif inventory_acronym == 'NEI':
            filter_path += 'NEI_pollutant_omit_list.csv'
            filter_type = 'drop'
        elif inventory_acronym == 'RCRAInfo':
            # drop records where 'Generator ID Included in NBR' != 'Y'
            # drop records where 'Generator Waste Stream Included in NBR' != 'Y'
            '''
            #Remove imported wastes, source codes G63-G75
            import_source_codes = pd.read_csv(rcra_data_dir + 'RCRAImportSourceCodes.txt',
                                            header=None)
            import_source_codes = import_source_codes[0].tolist()
            source_codes_to_keep = [x for x in BR['Source Code'].unique().tolist() if
                                    x not in import_source_codes]
            '''
            filter_type = 'drop'
        if filter_type is not None:
            inventory = filter_inventory(inventory, filter_path, 
                                         filter_type=filter_type)
    return inventory


def getInventoryFlows(inventory_acronym, year):
    """Returns flows for an inventory
    :param inventory_acronym: e.g. 'TRI'
    :param year: e.g. 2014
    :return: dataframe with standard flows format
    """
    flows = read_inventory(inventory_acronym + '_' + str(year), 'flow')
    return flows


def getInventoryFacilities(inventory_acronym, year):
    """Returns flows for an inventory
    :param inventory_acronym: e.g. 'TRI'
    :param year: e.g. 2014
    :return: dataframe with standard flows format
    """
    facilities = read_inventory(inventory_acronym + '_' + str(year), 'facility')
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
