# filter.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to support filtering of processed inventories
"""

import pandas as pd
from stewi.globals import data_dir, config, read_inventory, log

filter_config = config(file='filter.yaml')


def apply_filters_to_inventory(inventory, inventory_acronym, year, filter_list):
    """Apply one or more filters from a passed list to an inventory dataframe.

    :param inventory: df of stewi inventory of type flowbyfacility or flowbyprocess
    :param inventory_acronym: str of inventory e.g. 'NEI'
    :param year: year as number like 2010
    :param filter_list: a list of named filters to apply to inventory
    :return: DataFrame of filtered inventory
    """
    if 'filter_for_LCI' in filter_list:
        for name in filter_config['filter_for_LCI']['filters']:
            if name not in filter_list:
                filter_list.append(name)

    if 'US_States_only' in filter_list:
        log.info('filtering for US states')
        inventory = filter_states(inventory, inventory_acronym=inventory_acronym,
                                  year=year)

    if inventory_acronym == 'DMR':
        if 'remove_duplicate_organic_enrichment' in filter_list:
            from stewi.DMR import remove_duplicate_organic_enrichment
            inventory = remove_duplicate_organic_enrichment(inventory)

    if inventory_acronym == 'RCRAInfo':
        if 'National_Biennial_Report' in filter_list:
            log.info('filtering for National Biennial Report')
            fac_list = read_inventory('RCRAInfo', year, 'facility')
            fac_list = fac_list[['FacilityID',
                                 'Generator ID Included in NBR']
                                ].drop_duplicates(ignore_index=True)
            inventory = inventory.merge(fac_list, how='left')
            inventory = inventory[inventory['Generator ID Included in NBR'] == 'Y']
            inventory = inventory[inventory['Source Code'] != 'G61']
            inventory = inventory[inventory['Generator Waste Stream Included in NBR'] == 'Y']

        if 'imported_wastes' in filter_list:
            log.info('removing imported wastes')
            imp_source_codes = filter_config['imported_wastes']['parameters']['source_codes']
            inventory = inventory[~inventory['Source Code'].isin(imp_source_codes)]

    if 'flows_for_LCI' in filter_list:
        try:
            flow_filter_list = filter_config['flows_for_LCI']['parameters'][inventory_acronym]
        except KeyError:
            flow_filter_list = None
        if flow_filter_list is not None:
            log.info('removing flows not relevant for LCI')
            inventory = inventory[~inventory['FlowName'].isin(flow_filter_list)]

    return inventory


def filter_states(inventory_df, inventory_acronym=None, year=None,
                  include_states=True, include_dc=True, include_territories=False):
    """Remove records that are not included in the list of states.

    :param inventory_df: dataframe that includes column 'State' of 2 digit strings,
    if inventory_df does not contain 'State', inventory_acronym and year must be
    passed to retreive facility inventory
    :param include_states: bool, True to include data from 50 U.S. states
    :param include_dc: bool, True to include data from D.C.
    :param include_territories: bool, True to include data from U.S. territories
    :return: DataFrame
    """
    states_df = pd.read_csv(data_dir + 'state_codes.csv')
    states_list = []
    if 'State' not in inventory_df:
        if all(p is not None for p in [inventory_acronym, year]):
            fac_list = read_inventory(inventory_acronym, year, 'facility')
            fac_list = fac_list[['FacilityID', 'State']].drop_duplicates(ignore_index=True)
            inventory_df = inventory_df.merge(fac_list, how='left')
        else:
            log.warning('states cannot be assessed, no data removed')
            return inventory_df
    if include_states: states_list += list(states_df['states'].dropna())
    if include_dc: states_list += list(states_df['dc'].dropna())
    if include_territories: states_list += list(states_df['territories'].dropna())
    output_inventory = inventory_df[inventory_df['State'].isin(states_list)]
    return output_inventory
