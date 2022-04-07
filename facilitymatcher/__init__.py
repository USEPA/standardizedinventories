# __init__.py (facilitymatcher)
# !/usr/bin/env python3
# coding=utf-8
"""
Public API for facilitymatcher. Functions to match facilities across inventories
"""


from facilitymatcher.globals import filter_by_inventory_list, stewi_inventories,\
    filter_by_facility_list, filter_by_inventory_id_list, get_fm_file


def get_matches_for_inventories(inventory_list=stewi_inventories):
    """Return all facility matches for given inventories.

    :param inventory_list: list of inventories for desired matches using
        StEWI inventory names e.g. ['NEI','TRI']
    :return: dataframe in FacilityMatches standard output format
    """
    facilitymatches = get_fm_file('FacilityMatchList_forStEWI')
    facilitymatches = filter_by_inventory_list(facilitymatches, inventory_list)
    return facilitymatches


def get_FRS_NAICSInfo_for_facility_list(frs_id_list,
                                        inventories_of_interest_list=None,
                                        download_if_missing=False):
    """Return the FRS NAICS codes for the facilities of interest.

    Optionally it will also filter that FRS info by inventories of interest
    :param frs_id_list: list of FRS IDs
         e.g. ['110000491735', '110000491744']
    :param inventories_of_interest_list: list of inventories to filter NAICS
        info by using StEWI inventory names e.g. ['NEI']
    :param download_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: dataframe with columns 'FRS_ID', 'Source', 'NAICS',
        'PRIMARY_INDICATOR'
    """
    all_NAICS = get_fm_file('FRS_NAICSforStEWI', download_if_missing)
    if frs_id_list is not None:
        NAICS_of_interest = filter_by_facility_list(all_NAICS, frs_id_list)
    else:
        NAICS_of_interest = all_NAICS
    if inventories_of_interest_list is not None:
        NAICS_of_interest = filter_by_inventory_list(NAICS_of_interest,
                                                     inventories_of_interest_list)
    return NAICS_of_interest


def get_matches_for_id_list(base_inventory, id_list,
                            inventory_list=stewi_inventories):
    """Return facility matches given a list of inventories of interest,
    a base inventory and list of ids from that inventory.

    :param base_inventory: str base inventory corresponding to id_list (e.g. 'NEI)
    :param id_list: list of inventory ids (note not FRS_IDs)
         e.g. ['661411', '677611']
    :param inventory_list: list of inventories for desired matches
         using StEWI inventory names. defaults to all stewi inventories.
         e.g. ['NEI','TRI']
    :return: dataframe in FacilityMatches standard output format
    """
    return filter_by_inventory_id_list(get_fm_file('FacilityMatchList_forStEWI'),
                                       inventory_list, base_inventory, id_list)
