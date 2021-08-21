# filter.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to support filtering of processed inventories
"""

import pandas as pd
from stewi.globals import data_dir, import_table, config

filter_config = config(data_dir + 'filter.yaml')

def apply_filter_to_inventory(inventory, inventory_acronym, filter_list):
    # Apply filters if present
    if 'filter_for_LCI' in filter_list:
        for name in filter_config['filter_for_LCI']['filters']:
            if name not in filter_list:
                filter_list.append(name)

    if 'US_States_only' in filter_list:
        inventory = filter_states(inventory)

    if inventory_acronym == 'DMR':
        if 'remove_duplicate_organic_enrichment' in filter_list:
            from stewi.DMR import remove_duplicate_organic_enrichment
            inventory = remove_duplicate_organic_enrichment(inventory)

    if inventory_acronym == 'RCRAInfo':
        if 'National_Biennial_Report' in filter_list:
            '''
            BR = BR[BR['Source Code'] != 'G61']
            BR = BR[BR['Generator ID Included in NBR'] == 'Y']
            BR = BR[BR['Generator Waste Stream Included in NBR'] == 'Y']
            '''
        if 'imported_wastes' in filter_list:
            source_codes = filter_config['imported_wastes']['source_codes']
            '''
            BR = BR[BR['Source Code'].isin(source_codes_to_keep)]
            '''

    if 'flows_for_LCI' in filter_list:
        flow_filter_list = filter_config['flows_for_LCI'][inventory_acronym]
        filter_type = 'drop'
        inventory = filter_inventory(inventory, flow_filter_list, 
                                     filter_type=filter_type)
        
        # elif inventory_acronym == 'GHGRP':
        #     filter_path += 'ghg_mapping.csv'
        #     filter_type = 'keep'
        
    return inventory


def filter_inventory(inventory, criteria_table, filter_type, marker=None):
    """
    :param inventory_df: DataFrame to be filtered
    :param criteria_table: Can be a list of items to drop/keep, or a table
                        of FlowName, FacilityID, etc. with columns
                        marking rows to drop
    :param filter_type: drop, keep, mark_drop, mark_keep
    :param marker: Non-empty fields are considered marked by default.
        Option to specify 'x', 'yes', '1', etc.
    :return: DataFrame
    """
    inventory = import_table(inventory)
    criteria_table = import_table(criteria_table)
    if filter_type in ('drop', 'keep'):
        for criteria_column in criteria_table:
            for column in inventory:
                if column == criteria_column:
                    criteria = set(criteria_table[criteria_column])
                    if filter_type == 'drop':
                        inventory = inventory[~inventory[column].isin(criteria)]
                    elif filter_type == 'keep':
                        inventory = inventory[inventory[column].isin(criteria)]
    elif filter_type in ('mark_drop', 'mark_keep'):
        standard_format = import_table(data_dir + 'flowbyfacility_format.csv')
        must_match = standard_format['Name'][standard_format['Name'].isin(criteria_table.keys())]
        for criteria_column in criteria_table:
            if criteria_column in must_match: continue
            for field in must_match:
                if filter_type == 'mark_drop':
                    if marker is None:
                        inventory = inventory[~inventory[field].isin(
                            criteria_table[field][criteria_table[criteria_column] != ''])]
                    else:
                        inventory = inventory[~inventory[field].isin(
                            criteria_table[field][criteria_table[criteria_column] == marker])]
                if filter_type == 'mark_keep':
                    if marker is None:
                        inventory = inventory[inventory[field].isin(
                            criteria_table[field][criteria_table[criteria_column] != ''])]
                    else:
                        inventory = inventory[inventory[field].isin(
                            criteria_table[field][criteria_table[criteria_column] == marker])]
    return inventory.reset_index(drop=True)


def filter_states(inventory_df, include_states=True, include_dc=True,
                  include_territories=False):
    states_df = pd.read_csv(data_dir + 'state_codes.csv')
    states_filter = pd.DataFrame()
    states_list = []
    if include_states: states_list += list(states_df['states'].dropna())
    if include_dc: states_list += list(states_df['dc'].dropna())
    if include_territories: states_list += list(states_df['territories'].dropna())
    states_filter['State'] = states_list
    output_inventory = filter_inventory(inventory_df, states_filter, filter_type='keep')
    return output_inventory

