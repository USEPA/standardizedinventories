# __init__.py (stewicombo)
# !/usr/bin/env python3
# coding=utf-8
"""
Public API for stewicombo. Functions to combine inventory data
"""

import facilitymatcher
from stewicombo.overlaphandler import aggregate_and_remove_overlap
from stewicombo.globals import get_id_before_underscore,\
    getInventoriesforFacilityMatches, filter_by_primary_compartment,\
    addChemicalMatches, addBaseInventoryIDs, storeCombinedInventory,\
    write_stewicombo_metadata, compile_metadata, getCombinedInventory,\
    download_stewicombo_from_remote


def combineFullInventories(inventory_dict, filter_for_LCI=True,
                           remove_overlap=True, compartments=None,
                           **kwargs):
    """Combine full stewi inventories.

    :param inventory_dict: dictionary of inventories and years,
         e.g. {"TRI":"2014","NEI":"2014","RCRAInfo":"2015"}
    :param filter_for_LCI: boolean. Passes through to stewi to filter_for_LCI.
        See stewi.
    :param remove_overlap: boolean. Removes overlap across inventories
        based on preferences defined in globals
    :param compartments: list of compartments to include (e.g., 'water',
                                                          'air', 'land')
    :return: Flow-By-Facility Combined Format
    """
    inventory_acronyms = list(inventory_dict.keys())
    facilitymatches = facilitymatcher.get_matches_for_inventories(
        inventory_acronyms)
    inventories = getInventoriesforFacilityMatches(inventory_dict,
                                                   facilitymatches,
                                                   filter_for_LCI,
                                                   base_inventory=None,
                                                   **kwargs)
    if len(inventories) == 0:
        return None
    if compartments is not None:
        inventories = filter_by_primary_compartment(inventories, compartments)

    inventories = addChemicalMatches(inventories)

    # Aggregate and remove overlap if requested
    if remove_overlap:
        inventories = aggregate_and_remove_overlap(inventories)
        # For combined records, preserve record of that in 'FacilityIDs_Combined'
        inventories['FacilityIDs_Combined'] = inventories['FacilityID']
        # Otherwise take the first ID as the facility ID
        inventories['FacilityID'] = \
            inventories['FacilityID'].apply(lambda x: get_id_before_underscore(x))

    return inventories


def combineInventoriesforFacilitiesinBaseInventory(base_inventory,
                                                   inventory_dict,
                                                   filter_for_LCI=True,
                                                   remove_overlap=True):
    """Combine stewi inventories for all facilities present in base_inventory.

    The base_inventory must be in the inventory_dict
    :param base_inventory: reference inventory e.g. "TRI"
    :param inventory_dict: dictionary of inventories and years,
         e.g. {"TRI":"2014","NEI":"2014","RCRAInfo":"2015"}
    :param filter_for_LCI: boolean. Passes through to stewi to filter_for_LCI.
        See stewi.
    :param remove_overlap: boolean. Removes overlap across inventories
        based on preferences defined in globals
    :return: Flow-By-Facility Combined Format
    """
    inventory_acronyms = list(inventory_dict.keys())
    facilitymatches = facilitymatcher.get_matches_for_inventories(
        inventory_acronyms)
    inventories = getInventoriesforFacilityMatches(inventory_dict,
                                                   facilitymatches,
                                                   filter_for_LCI,
                                                   base_inventory)
    inventories = addChemicalMatches(inventories)

    # Aggregate and remove overlap if requested
    if remove_overlap:
        inventories = aggregate_and_remove_overlap(inventories)

    inventories = addBaseInventoryIDs(inventories, facilitymatches,
                                      base_inventory)
    return inventories


def combineInventoriesforFacilityList(base_inventory, inventory_dict,
                                      facility_id_list,
                                      filter_for_LCI=True, remove_overlap=True):
    """Combine inventories for all facilities present in facility id list for base_inventory.

    The base_inventory must be in the inventory_dict
    :param base_inventory: reference inventory e.g. "TRI"
    :param inventory_dict: dictionary of inventories and years,
         e.g. {"TRI":"2014","NEI":"2014","RCRAInfo":"2015"}
    :param facility_id_list: list of facility ids from base_inventory
         e.g. ['99501MPCLS1076O', '99501NCHRG459WB', '99515VNWTR590E1']
    :param filter_for_LCI: boolean. Passes through to stewi to filter_for_LCI.
        See stewi.
    :param remove_overlap: boolean. Removes overlap across inventories
        based on preferences defined in globals
    :return: Flow-By-Facility Combined Format
    """
    inventory_acronyms = list(inventory_dict.keys())
    facilitymatches = facilitymatcher.get_matches_for_id_list(
        base_inventory, facility_id_list, inventory_acronyms)
    inventories = getInventoriesforFacilityMatches(inventory_dict,
                                                   facilitymatches,
                                                   filter_for_LCI,
                                                   base_inventory)
    # Remove the records from the base_inventory that are not in the
    # facility list
    remove_records = inventories[(inventories['Source'] == base_inventory) &
        (~inventories['FacilityID'].isin(facility_id_list))].index
    inventories = inventories.drop(remove_records, axis=0)
    # Add in chemical matches
    inventories = addChemicalMatches(inventories)

    # Aggregate and remove overlap if requested
    if remove_overlap:
        inventories = aggregate_and_remove_overlap(inventories)

    inventories = addBaseInventoryIDs(inventories, facilitymatches,
                                      base_inventory)
    return inventories


def saveInventory(name, combinedinventory_df, inventory_dict):
    """Save a combined inventory in local directory with metadata.

    :param name: str, desired name for dataset e.g. 'CAP_HAP_national_2017'
    :param combinedinventory_df: df to save
    :param inventory_dict: dictionary of inventories and years, used to compile
        metadata for saved inventory
    """
    storeCombinedInventory(combinedinventory_df, name)
    inventory_meta = compile_metadata(inventory_dict)
    write_stewicombo_metadata(name, inventory_meta)


def getInventory(name, download_if_missing=False):
    """Retrieve locally stored inventory in Flow-By-Facility Combined Format.

    :param name: str, name of dataset or name of file, e.g.
        'CAP_HAP_national_2017' or 'CAP_HAP_national_2017_v0.9.7_5cf36c0.parquet'
    :param download_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: combined inventory as dataframe
    """
    combinedinventory_df = getCombinedInventory(name)
    if combinedinventory_df is None and download_if_missing:
        download_stewicombo_from_remote(name)
        combinedinventory_df = getCombinedInventory(name)
    return combinedinventory_df


def pivotCombinedInventories(combinedinventory_df):
    """Create a pivot table of combined emissions.

    :param combinedinventory_df: pandas dataframe returned from a
        'combineInventories..' function
    :return: pandas pivot_table
    """
    # Group the results by facility,flow,and compartment
    combinedinventory_df_pt = combinedinventory_df.pivot_table(
        values=['FlowAmount', 'DataReliability'],
        index=['FRS_ID', 'SRS_ID', 'Compartment'],
        columns='Source')
    return combinedinventory_df_pt
