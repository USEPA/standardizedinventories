""" Handle redundant flows by facility and compartment """

from pathlib import Path

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


def remove_flow_overlap(df, flow_cpst, flows_cntb, cmpt='air', SCC=False):
    """
    Subtract double-counted contributing flow quantities from a composite flow.
    E.g., remove PM2.5 quantities from PM10 (i.e., all PM <= 10, by default)
    to produce PM10-PM2.5 (2.5 < PM <= 10).
    :param df: pd.DataFrame, flow-by-facility combined format
    :param flow_cpst: str, composite flow SRS_ID code
    :param flows_cntb: list, contributing flows (SRS_ID codes) to
        remove from a composite flow
    :param cmpt: str, primary compartment containing flow overlap
    :param SCC: bool, optionally aggregate contributing flows at process level
    """
    cols_agg = ['FacilityID', 'Source', 'Compartment']
    if SCC:
        cols_agg.append('Process')
    # sum contributing flows' FlowAmounts by cols_agg (i.e., across SRS_IDs)
    if '_CompartmentPrimary' not in df:
        df['_CompartmentPrimary'] = df['Compartment'].apply(lambda x: x.split('/')[0])
    df_cf = (df.query('SRS_ID in @flows_cntb and '
                      '_CompartmentPrimary == @cmpt')
               .groupby(cols_agg, as_index=False)
               .agg({'FlowAmount': 'sum'})
               .assign(SRS_ID=flow_cpst)
               .rename(columns={'FlowAmount': 'ContributingAmount'}))
    # then remove contributing flow totals from composite flow
    df = (df.merge(df_cf, how='left', on=(cols_agg + ['SRS_ID']))
            .fillna({'ContributingAmount': 0})
            .eval('FlowAmount = FlowAmount - ContributingAmount')
            .drop(columns='ContributingAmount'))
    # and ensure the adjusted composite flows are non-negative
    cond = (df['FlowAmount'] < 0) & (df['SRS_ID'] == flow_cpst)
    df['FlowAmount'] = df['FlowAmount'].mask(cond, 0)
    return df


def remove_NEI_overlaps(df, **kwargs):
    """
    Remove overlaps inherent to NEI's default PM and VOC flows.
    :param df: pd.DataFrame, flow-by-facility combined format
    """
    log.info('Assessing PM and VOC speciation')
    # Remove PM2.5-PRI (SRS_ID: 77681) from PM10-PRI (SRS_ID: 77683)
    df = remove_flow_overlap(df, '77683', ['77681'], **kwargs)
    df.loc[(df['SRS_ID'] == '77683'), 'FlowName'] = 'PM10-PM2.5'
    # Remove speciated HAP VOCs (import SRSs) from composite VOC flows (SRS_ID: 83723)
    # Defined in EPA's Industrial, Commercial, and Institutional (ICI)
    # Fuel Combustion Tool, Version 1.4, December 2015
    # (Available at: ftp://ftp.epa.gov/EmisInventory/2014/doc/nonpoint/ICI%20Tool%20v1_4.zip).
    VOC_srs = (pd.read_csv(path_data / 'VOC_SRS_IDs.csv', dtype=str)
                  .squeeze()) # single-col df to series
    df = remove_flow_overlap(df, '83723', VOC_srs, **kwargs)
    return df


def remove_default_flow_overlaps(df, **kwargs):
    from warnings import warn
    warn('remove_default_flow_overlaps() is deprecated. \n'
         'Replace with remove_NEI_overlaps()', DeprecationWarning)
    return remove_NEI_overlaps(df, **kwargs)


def aggregate_and_remove_overlap(df):
    """
    Aggregate or remove redundant flows (preferences given in inv_pref)
    by facility and compartment
    :param df: pd.DataFrame, inventory df incl. chemical & facility matches
    """
    log.info('removing overlap between inventories')
    ## TODO: implement args for different duplicate handling schemes
        # see commented-out code in commit f2fc7c2 (or earlier, uncommented)

    df['_CompartmentPrimary'] = df['Compartment'].apply(lambda x: x.split('/')[0])

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

    # minimal cols to find duplicated flows ACROSS inventories; excludes
    # 'Compartment' b/c secondary contexts vary w/ data availability
    cols_inter = ['FRS_ID', 'SRS_ID', '_CompartmentPrimary']

    # split into df's w/ unique (unq) and duplicated (dup) flows
    id_duplicates = df.duplicated(subset=cols_inter, keep=False)
    df_unq = df[~id_duplicates]
    df_dup = df.copy()[id_duplicates]

    # functions by column for intra-inventory aggregation
    funcs_agg = {
        'FacilityID':       '_'.join, # or `set` or `'unique'` to get unique set of vals
        'FlowAmount':       'sum',
        'DataReliability':  'sum',  # sums FlowAmount-weighted elements
        'FlowName':         'first', # get the first element in .agg
        }
    # cols to define unique flows WITHIN inventories; using more grouping cols,
    # rather than minimal cols + select unique elements, speeds up .agg()
    cols_intra = list(set(df_dup.columns) - set(funcs_agg.keys()))
    # affirm that cols_intra produces same number of groups as minimal cols
    cols_min = cols_inter + ['Source', 'Compartment', 'FlowName']
    if not (df_dup.groupby(cols_intra).ngroups == df_dup.groupby(cols_min).ngroups):
        log.error('intra-inventory unique-flow-defining cols are insufficient')

    df_dup['_FlowAmountSum'] = (df_dup.groupby(cols_intra)['FlowAmount']
                                      .transform('sum'))
    df_dup['DataReliability'] = df_dup.eval(
        'DataReliability * FlowAmount / _FlowAmountSum')
    df_dup = df_dup.groupby(cols_intra, as_index=False).agg(funcs_agg)

    # get source preference score (ordered, integer positions in inv_pref tuples)
    # via each entry's compartment and inventory source acronym

    df_dup['_SourcePref'] = df_dup.apply(
        lambda x: inv_pref.get(x['_CompartmentPrimary']).index(x['Source']),
        axis='columns')
    # df_dup.groupby(['Source', '_CompartmentPrimary', '_SourcePref']).size()

    # then drop cross-inventory dups by keeping entries w/ min _SourcePref
    df_dup['_SourcePrefMin'] = (df_dup.groupby(cols_inter)['_SourcePref']
                                      .transform('min'))
    df_dup = df_dup.query('_SourcePref == _SourcePrefMin')

    log.debug('Reincorporating rows with NaN FRS_ID or SRS_ID')
    df = pd.concat([df_unq, df_dup, df_nans], ignore_index=True)

    if 'NEI' in df['Source'].values:
        df = remove_NEI_overlaps(df)

    df = df.filter(regex=r'^(?!_)') # only keep cols not starting w/ '_'

    log.info('overlap removed')
    return df
