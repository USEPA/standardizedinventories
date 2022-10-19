# __init__.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Public API for stewi. Functions to return inventory data for a single
inventory in standard formats
"""


from esupy.processed_data_mgmt import read_source_metadata
from stewi.globals import log, add_missing_fields,\
    WRITE_FORMAT, read_inventory, paths,\
    set_stewi_meta, aggregate
from stewi.filter import apply_filters_to_inventory, filter_config
from stewi.formats import StewiFormat, ensure_format


def getAvailableInventoriesandYears(stewiformat='flowbyfacility'):
    """Get available inventories and years for a given output format.

    :param stewiformat: str e.g. 'flowbyfacility'
    :return: existing_inventories dictionary of inventories like:
        {NEI: [2014],
         TRI: [2015, 2016]}
    """
    f = ensure_format(stewiformat)
    if f.path().is_dir():
        files = [x for x in f.path().glob(f"*.{WRITE_FORMAT}") if x.is_file()]
    else:
        log.error(f'directory not found: {f.path()}')
        return
    outputfiles = []
    for name in files:
        if '_v' in name.stem:
            _n = name.stem[:name.stem.find('_v')]
        outputfiles.append(tuple(_n.split('_')))
    # remove duplicates
    outputfiles = list(set(outputfiles))
    existing_inventories = {}
    for acronym, year in outputfiles:
        if acronym not in existing_inventories.keys():
            existing_inventories[acronym] = [year]
        else:
            existing_inventories[acronym].append(year)
    for key in existing_inventories.keys():
        existing_inventories[key].sort()
    return existing_inventories


def printAvailableInventories(stewiformat='flowbyfacility'):
    """Print available inventories and years for a given output format.

    :param stewiformat: str e.g. 'flowbyfacility' or 'flow'
    """
    existing_inventories = getAvailableInventoriesandYears(stewiformat)
    if existing_inventories == {}:
        print('No inventories found')
    else:
        print(f'{stewiformat} inventories available (name, year):')
        print("\n".join([f'{k}: {", ".join(v)}' for k, v in existing_inventories.items()]))


def getInventory(inventory_acronym, year, stewiformat='flowbyfacility',
                 filters=None, filter_for_LCI=False, US_States_Only=False,
                 download_if_missing=False, keep_sec_cntx=True):
    """Return or generate an inventory in a standard output format.

    :param inventory_acronym: like 'TRI'
    :param year: year as number like 2010
    :param stewiformat: str e.g. 'flowbyfacility' or 'flow'
    :param filters: a list of named filters to apply to inventory
    :param filter_for_LCI: whether or not to filter inventory for life
        cycle inventory creation, is DEPRECATED in favor of 'filters'
    :param US_States_Only: includes only US states, is DEPRECATED in
        favor of 'filters'
    :param download_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :param keep_sec_cntx: bool, if False will collapse secondary contexts
        (e.g., rural or urban, or release height)
    :return: dataframe with standard fields depending on output format
    """
    f = ensure_format(stewiformat)
    inventory = read_inventory(inventory_acronym, year, f,
                               download_if_missing)

    if (not keep_sec_cntx) and ('Compartment' in inventory):
        inventory['Compartment'] = (inventory['Compartment']
                                    .str.partition('/')[0])
        inventory = aggregate(inventory)

    if not filters:
        filters = []
    if f.value > 2:  # exclude FLOW and FACILITY
        # for backwards compatability, maintain these optional parameters in getInventory
        if filter_for_LCI:
            log.warning(r'"filter_for_LCI" parameter is deprecated and will be removed '
                        'as a paramter in getInventory in future release.\n'
                        r'Add "filter_for_LCI" to filters.')
            if 'filter_for_LCI' not in filters:
                filters.append('filter_for_LCI')
        if US_States_Only:
            log.warning(r'"US_States_Only" parameter is deprecated and will be removed '
                        'as a paramter in getInventory in future release.\n'
                        r'Add "US_States_only" to filters.')
            if 'US_States_only' not in filters:
                filters.append('US_States_only')

        inventory = apply_filters_to_inventory(inventory, inventory_acronym, year,
                                               filters, download_if_missing)
        # After filting, may be necessary to reaggregate inventory again
        inventory = aggregate(inventory)

    inventory = add_missing_fields(inventory, inventory_acronym, f,
                                   maintain_columns=False)

    return inventory


def getInventoryFlows(inventory_acronym, year,
                      download_if_missing=False):
    """Return flows for an inventory.

    :param inventory_acronym: e.g. 'TRI'
    :param year: e.g. 2014
    :param download_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: dataframe with standard flows format
    """
    flows = read_inventory(inventory_acronym, year, StewiFormat.FLOW,
                           download_if_missing)
    if flows is None:
        return
    flows = add_missing_fields(flows, inventory_acronym, StewiFormat.FLOW,
                               maintain_columns=False)
    return flows


def getInventoryFacilities(inventory_acronym, year,
                           download_if_missing=False):
    """Return flows for an inventory.

    :param inventory_acronym: e.g. 'TRI'
    :param year: e.g. 2014
    :param download_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: dataframe with standard flows format
    """
    facilities = read_inventory(inventory_acronym, year, StewiFormat.FACILITY,
                                download_if_missing)
    if facilities is None:
        return
    facilities = add_missing_fields(facilities, inventory_acronym, StewiFormat.FACILITY,
                                    maintain_columns=True)
    return facilities


def getMetadata(inventory_acroynym, year):
    """Return metadata in the form of a dictionary as read from stored JSON file.

    :param inventory_acronym: e.g. 'TRI'
    :param year: e.g. 2014
    :return: metadata dictionary
    """
    meta = read_source_metadata(paths,
                                set_stewi_meta(f"{inventory_acroynym}_{str(year)}"),
                                force_JSON=True)
    return meta


def seeAvailableInventoryFilters():
    """Print available filters for use in getInventory."""
    for f in filter_config:
        print(f"{f}: {filter_config[f]['description']}")
        if (filter_config[f]['type'] == 'set'):
            print('Includes the following filters: ' + ', '.join(
                filter_config[f]['filters']))
