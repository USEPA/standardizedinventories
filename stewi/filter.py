# filter.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to support filtering of processed inventories
"""

import pandas as pd
from stewi.globals import DATA_PATH, config, read_inventory, log
from stewi.formats import StewiFormat

filter_config = config(file='filter.yaml')


def apply_filters_to_inventory(inventory, inventory_acronym, year, filters,
                               download_if_missing=False):
    """Apply one or more filters from a passed list to an inventory dataframe.

    :param inventory: df of stewi inventory of type flowbyfacility or flowbyprocess
    :param inventory_acronym: str of inventory e.g. 'NEI'
    :param year: year as number like 2010
    :param filters: a list of named filters to apply to inventory
    :param download_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: DataFrame of filtered inventory
    """
    if 'filter_for_LCI' in filters:
        for name in filter_config['filter_for_LCI']['filters']:
            if name not in filters:
                filters.append(name)
    compare_to_available_filters(filters)

    if 'US_States_only' in filters:
        log.info('filtering for US states')
        inventory = filter_states(inventory, inventory_acronym=inventory_acronym,
                                  year=year,
                                  download_if_missing=download_if_missing)

    if inventory_acronym == 'DMR' and 'remove_duplicate_organic_enrichment' in filters:
        from stewi.DMR import remove_duplicate_organic_enrichment
        inventory = remove_duplicate_organic_enrichment(inventory)

    if inventory_acronym == 'RCRAInfo' and 'National_Biennial_Report' in filters:
        log.info('filtering for National Biennial Report')
        fac_list = read_inventory('RCRAInfo', year, StewiFormat.FACILITY,
                                  download_if_missing)
        fac_list = fac_list[['FacilityID',
                             'Generator ID Included in NBR']
                            ].drop_duplicates(ignore_index=True)
        inventory = inventory.merge(fac_list, how='left')
        inventory = inventory[inventory['Generator ID Included in NBR'] == 'Y']
        inventory = inventory[inventory['Source Code'] != 'G61']
        inventory = inventory[inventory['Generator Waste Stream Included in NBR'] == 'Y']

    if inventory_acronym == 'RCRAInfo' and 'imported_wastes' in filters:
        log.info('removing imported wastes')
        imp_source_codes = filter_config['imported_wastes']['parameters']['source_codes']
        inventory = inventory[~inventory['Source Code'].isin(imp_source_codes)]

    if 'flows_for_LCI' in filters:
        flow_filter_list = filter_config['flows_for_LCI']['parameters'].get(inventory_acronym)
        if flow_filter_list is not None:
            log.info('removing flows not relevant for LCI')
            inventory = inventory[~inventory['FlowName'].isin(flow_filter_list)]

    return inventory


def filter_states(inventory_df, inventory_acronym=None, year=None,
                  include_states=True, include_dc=True, include_territories=False,
                  download_if_missing=False):
    """Remove records that are not included in the list of states.

    :param inventory_df: dataframe that includes column 'State' of 2 digit strings,
    if inventory_df does not contain 'State', inventory_acronym and year must be
    passed to retreive facility inventory
    :param include_states: bool, True to include data from 50 U.S. states
    :param include_dc: bool, True to include data from D.C.
    :param include_territories: bool, True to include data from U.S. territories
    :param download_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: DataFrame
    """
    states_df = pd.read_csv(DATA_PATH.joinpath('state_codes.csv'))
    states_list = []
    if 'State' not in inventory_df:
        if all(p is not None for p in [inventory_acronym, year]):
            fac_list = read_inventory(inventory_acronym, year, StewiFormat.FACILITY,
                                      download_if_missing)
            fac_list = fac_list[['FacilityID', 'State']].drop_duplicates(ignore_index=True)
            inventory_df = inventory_df.merge(fac_list, how='left')
        else:
            log.warning('states cannot be assessed, no data removed')
            return inventory_df
    if include_states:
        states_list += list(states_df['states'].dropna())
    if include_dc:
        states_list += list(states_df['dc'].dropna())
    if include_territories:
        states_list += list(states_df['territories'].dropna())
    output_inventory = inventory_df[inventory_df['State'].isin(states_list)]
    return output_inventory


def compare_to_available_filters(filters):
    """Compare passed filters to available filters in filter.yaml."""
    x = [s for s in filters if s not in filter_config.keys()]
    if x:
        log.warning(f"the following filters are unavailable: {', '.join(x)}")
