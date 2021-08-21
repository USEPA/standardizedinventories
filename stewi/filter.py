# filter.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to support filtering of processed inventories
"""

import pandas as pd
from stewi.globals import data_dir, import_table


def apply_filter_to_inventory(inventory, inventory_acronym, filter_for_LCI,
                              US_States_Only):
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
            filter_type = 'drop'
            '''
        if filter_type is not None:
            inventory = filter_inventory(inventory, filter_path, 
                                         filter_type=filter_type)
    return inventory


def filter_inventory(inventory, criteria_table, filter_type, marker=None):
    """
    :param inventory_df: DataFrame to be filtered
    :param criteria_file: Can be a list of items to drop/keep, or a table
                        of FlowName, FacilityID, etc. with columns
                        marking rows to drop
    :param filter_type: drop, keep, mark_drop, mark_keep
    :param marker: Non-empty fields are considered marked by default.
        Option to specify 'x', 'yes', '1', etc.
    :return: DataFrame
    """
    inventory = import_table(inventory); criteria_table = import_table(criteria_table)
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

