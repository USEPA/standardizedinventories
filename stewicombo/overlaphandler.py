""" Handle redundant flows by facility and compartment """

from pathlib import Path

import numpy as np
import pandas as pd

from stewi.globals import log


path_module = Path(__file__).parent
path_data = path_module / 'data'

inv_pref = {  # inventory source preference by compartment
    'air': ('eGRID', 'GHGRP', 'NEI', 'TRI'),
    'water': ('DMR', 'TRI'),
    'soil': ('TRI'),
    'waste': ('RCRAInfo', 'TRI'),
    'output': ('eGRID'),
    }


def remove_flow_overlap(df, aggregate_flow, contributing_flows,
                        primary_compartment='air', SCC=False):
    df_contributing_flows = df.loc[df['SRS_ID'].isin(contributing_flows)]
    df_contributing_flows = df_contributing_flows[df_contributing_flows[
        'Compartment'].str.partition('/')[0] == primary_compartment]
    match_conditions = ['FacilityID', 'Source', 'Compartment']
    if SCC:
        match_conditions.append('Process')

    df_contributing_flows = df_contributing_flows.groupby(
        match_conditions, as_index=False)['FlowAmount'].sum()

    df_contributing_flows['SRS_ID'] = aggregate_flow
    df_contributing_flows['ContributingAmount'] = df_contributing_flows['FlowAmount']
    df_contributing_flows.drop(columns=['FlowAmount'], inplace=True)
    df = df.merge(df_contributing_flows, how='left', on=match_conditions.append('SRS_ID'))
    df[['ContributingAmount']] = df[['ContributingAmount']].fillna(value=0)
    df['FlowAmount'] = df['FlowAmount'] - df['ContributingAmount']
    df.drop(columns=['ContributingAmount'], inplace=True)

    # Make sure the aggregate flow is non-negative
    df.loc[((df.SRS_ID == aggregate_flow) & (df.FlowAmount <= 0)), 'FlowAmount'] = 0
    return df


def remove_default_flow_overlaps(df, compartment='air', SCC=False):
    log.info('Assessing PM and VOC speciation')

    # SRS_ID = 77683 (PM10-PRI) and SRS_ID = 77681  (PM2.5-PRI)
    df = remove_flow_overlap(df, '77683', ['77681'], compartment, SCC)
    df.loc[(df['SRS_ID'] == '77683'), 'FlowName'] = 'PM10-PM2.5'

    # SRS_ID = 83723 (VOC) change FlowAmount by subtracting sum of FlowAmount from speciated HAP VOCs.
    # The records for speciated HAP VOCs are not changed.
    # Defined in EPA’s Industrial, Commercial, and Institutional (ICI) Fuel Combustion Tool, Version 1.4, December 2015
    # (Available at: ftp://ftp.epa.gov/EmisInventory/2014/doc/nonpoint/ICI%20Tool%20v1_4.zip).
        # TODO: update link? no longer works
    VOC_srs = (pd.read_csv(path_data / 'VOC_SRS_IDs.csv', dtype=str)
                 .squeeze()) # single col df to series
    df = remove_flow_overlap(df, '83723', VOC_srs, compartment, SCC)
    return df


def aggregate_and_remove_overlap(df):
    """
    Aggregate or remove redundant flows (preferences given in inv_pref)
    by facility and compartment
    :param df: pd.DataFrame, inventory df incl. chemical & facility matches
    """
    log.info('removing overlap between inventories')
    ## TODO: implement args for different duplicate handling schemes
    # KEEP_ALL_DUPLICATES = True
    # INCLUDE_ORIGINAL = True
    # KEEP_ROW_WITHOUT_DUPS = True
    # if not INCLUDE_ORIGINAL and not KEEP_ALL_DUPLICATES:
    #     raise ValueError('Cannot have both INCLUDE_ORIGINAL and '
    #                      'KEEP_REPEATED_DUPLICATES fields as False')
    # if INCLUDE_ORIGINAL:
    #     keep = False
    # else:
    #     keep = 'first'
    # # if you wish to also keep row that doesn't have any duplicates, don't find duplicates
    # # go ahead with next step of processing
    # if not KEEP_ROW_WITHOUT_DUPS:
    #     df_chunk_filtered = df[cols_intra]
    #     if not KEEP_ALL_DUPLICATES:
    #     # from a set of duplicates a logic is applied to figure out what is sent to write to output file
    #         # for example only the first duplicate is kept
    #         # or duplicates are filtered preferentially and high priority one is kept etc
    #         df_dup = df[df_chunk_filtered.duplicated(keep=keep)]
    #         df_dup_filtered = df_dup[cols_intra]
    #         df = df_dup[df_dup_filtered.duplicated(keep=keep).apply(lambda x: not x)]

    # split off rows w/ NaN FRS_ID or SRS_ID & later recombine into output
    df_nans = df.query('FRS_ID.isnull() or SRS_ID.isnull()')
    # convert numeric code fields (str & float mix) to str
    fields_as_str = ['FRS_ID', 'SRS_ID', 'SRS_CAS']
    df[fields_as_str] = df[fields_as_str].astype(str)

    # adjust special use case for flows in TRI and DMR
    if 'DMR' in df['Source'].values and 'TRI' in df['Source'].values:
        from stewi.DMR import remove_nutrient_overlap_TRI
        df = remove_nutrient_overlap_TRI(df, inv_pref['water'][0])

    # drop split NaN rows
    df = df.drop(df_nans.index)
    df['_CompartmentPrimary'] = df['Compartment'].apply(lambda x: x.split('/')[0])

    # minimal cols to find duplicated flows ACROSS inventories; excludes
    # 'Compartment' b/c secondary contexts vary w/ data availability
    cols_inter = ['FRS_ID', 'SRS_ID', '_CompartmentPrimary']

    # split into df's w/ unique (unq) and duplicated (dup) flows
    id_duplicates = df.duplicated(subset=cols_inter, keep=False)
    df_unq = df[~id_duplicates]
    df_dup = df.copy()[id_duplicates]

    func_cols_map = {
        'FacilityID':       '_'.join, # or `set` or `'unique'` to get unique set of vals
        'FlowAmount':       sum,
        'DataReliability':  sum,  # sums FlowAmount-weighted elements
        'FlowName':         'first', # get the first element in .agg
        }

    # cols to define unique flows WITHIN inventories; rather than use a minimal
    # set of cols & select unique elements, using more grouping cols speeds up .agg()
    cols_intra = list(set(df_dup.columns) - set(func_cols_map.keys()))
    # affirm that cols_intra produces same # of groups as minimal cols
    if not (len(df_dup.groupby(cols_intra)) ==
            len(df_dup.groupby(cols_inter + ['Source', 'Compartment', 'FlowName']))):
        log.error('intra-inventory unique-flow-defining cols are insufficient')

    df_dup['_FlowAmountSum'] = df_dup.groupby(cols_intra)['FlowAmount'].transform('sum')
    df_dup['DataReliability'] = df_dup.eval('DataReliability * FlowAmount / _FlowAmountSum')
    df_dup = df_dup.groupby(cols_intra, as_index=False).agg(func_cols_map)

    # get source preference score (ordered, integer positions in inv_pref tuples)
    # via each entry's compartment and inventory source acronym
    df_dup['_SourcePref'] = df_dup.apply(
        lambda x: inv_pref.get(x['_CompartmentPrimary']).index(x['Source']),
        axis='columns')
    # df_dup.groupby(['Source', '_CompartmentPrimary', '_SourcePref']).size()

    # then drop cross-inventory dups by keeping entries w/ min _SourcePref
    df_dup['_SourcePrefMin'] = df_dup.groupby(cols_inter)['_SourcePref'].transform(min)
    df_dup = df_dup.query('_SourcePref == _SourcePrefMin')

    log.debug('Reincorporating rows with NaN FRS_ID or SRS_ID')
    df = (pd.concat([df_unq, df_dup, df_nans], ignore_index=True)
            .filter(regex=r'^(?!_)')) # only keep cols not starting w/ '_'

    if 'NEI' in df['Source'].values:
        df = remove_default_flow_overlaps(df, compartment='air', SCC=False)

    log.info('overlap removed')
    return df


if __name__ == '__main__':
    # %% working
    from pandas.testing import assert_frame_equal
    import facilitymatcher
    from stewicombo.globals import getInventoriesforFacilityMatches, \
        addChemicalMatches  #, filter_by_primary_compartment

    inventory_dict = {"NEI": "2017", "TRI": "2017"}
    # inventory_dict = {"TRI": "2017", "DMR": "2017"}
    facilitymatches = facilitymatcher.get_matches_for_inventories(
        list(inventory_dict.keys()))
    df = getInventoriesforFacilityMatches(inventory_dict,
                                          facilitymatches,
                                          filter_for_LCI=True,
                                          base_inventory=None)
    # if compartments is not None:
    #     inventories = filter_by_primary_compartment(inventories, compartments)
    df = addChemicalMatches(df)

    # df_agg = aggregate_and_remove_overlap(df)

    # df = df[df['SRS_ID'].isin(['77683', '77681'])]
    # df = remove_flow_overlap(df, '77683', ['77681'],
    #                          primary_compartment='air', SCC=False)

    # %% inventory gen test
    # import stewicombo
    # def test_generate_combined_inventories(name, compartment, inv_dict):
    #     df = stewicombo.combineFullInventories(inv_dict,
    #                                            filter_for_LCI=True,
    #                                            remove_overlap=True,
    #                                            compartments=[compartment])
    #     stewicombo.saveInventory(name, df, inv_dict)
    #     df2 = stewicombo.getInventory(name, download_if_missing=False)
    #     assert df2 is not None
    #     return df2
    # df = test_generate_combined_inventories("NEI_TRI_air_2017",
    #                                         "air",
    #                                         {"NEI": "2017", "TRI": "2017"})
