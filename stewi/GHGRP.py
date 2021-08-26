# GHGRP.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Imports GHGRP data and processes to Standardized EPA output format.
This file requires parameters be passed like:

    Option -Y Year 

Options:
    A - for downloading and processing GHGRP data from web and saving locally
    B - for generating inventory files for StEWI:
         - flowbysubpart
         - flowbyfacility
         - flows
         - facilities
        and validating flowbyfacility against national totals
    C - for downloading national totals for validation

Year: 
    2019
    2018
    2017
    2016
    2015   
    2014
    2013
    2012
    2011
    
Models with tables available at:
    https://www.epa.gov/enviro/greenhouse-gas-model
Envirofacts web services documentation can be found at:
    https://www.epa.gov/enviro/web-services
"""

import pandas as pd
import numpy as np
import requests
from xml.dom import minidom
import time
import os
import argparse

from stewi.globals import download_table,\
    write_metadata, import_table, drop_excel_sheets,\
    data_dir, get_reliability_table_for_source, set_stewi_meta,\
    flowbyfacility_fields, flowbyprocess_fields, facility_fields, config,\
    store_inventory, paths, log, create_paths_if_missing,\
    compile_source_metadata, read_source_metadata, aggregate
from stewi.validate import update_validationsets_sources, validate_inventory,\
    write_validation_result
    

_config = config()['databases']['GHGRP']
ghgrp_data_dir = data_dir + 'GHGRP/'
ext_folder = 'GHGRP Data Files'
ghgrp_external_dir = paths.local_path + '/' + ext_folder + '/'
   
# Flow codes that are reported in validation in CO2e
flows_CO2e = ['PFC', 'HFC', 'Other','Very_Short', 'HFE', 'Other_Full']

# define GWPs
# (these values are from IPCC's AR4, which is consistent with GHGRP methodology)
CH4GWP = 25
N2OGWP = 298
HFC23GWP = 14800

# define column groupings
ghgrp_cols = import_table(ghgrp_data_dir + 'ghgrp_columns.csv')
name_cols = list(ghgrp_cols[ghgrp_cols['ghg_name'] == 1]['column_name'])
alias_cols = list(ghgrp_cols[ghgrp_cols['ghg_alias'] == 1]['column_name'])
quantity_cols = list(ghgrp_cols[ghgrp_cols['ghg_quantity'] == 1]['column_name'])
co2_cols = list(ghgrp_cols[ghgrp_cols['co2'] == 1]['column_name'])
ch4_cols = list(ghgrp_cols[ghgrp_cols['ch4'] == 1]['column_name'])
n2o_cols = list(ghgrp_cols[ghgrp_cols['n2o'] == 1]['column_name'])
co2e_cols = list(ghgrp_cols[ghgrp_cols['co2e_quantity'] == 1]['column_name'])
subpart_c_cols = list(ghgrp_cols[ghgrp_cols['subpart_c'] == 1]['column_name'])
method_cols = list(ghgrp_cols[ghgrp_cols['method'] == 1]['column_name'])
base_cols = list(ghgrp_cols[ghgrp_cols['base_columns'] == 1]['column_name'])
info_cols = name_cols + quantity_cols + method_cols
group_cols = co2_cols + ch4_cols + n2o_cols 
ghg_cols = base_cols + info_cols + group_cols

# define filepaths for downloaded data
data_summaries_path = ghgrp_external_dir + _config['most_recent_year'] +\
    '_data_summary_spreadsheets/'
esbb_subparts_path = ghgrp_external_dir + _config['esbb_subparts_url']
lo_subparts_path = ghgrp_external_dir + _config['lo_subparts_url']
   
# set format for metadata file
ghgrp_metadata = {} 
ghgrp_metadata['time_meta'] = []
ghgrp_metadata['filename_meta'] = []
ghgrp_metadata['type_meta'] = []
ghgrp_metadata['url_meta'] = []

def generate_url(table, report_year='', row_start=0, row_end=9999,
                 output_ext='JSON'):
    '''Input a specific table name to generate the query URL to submit'''
    request_url = _config['enviro_url'] + table
    if report_year != '': request_url += '/REPORTING_YEAR/=/' + report_year
    if row_start != '': request_url += '/ROWS/' + str(row_start) + ':' +\
        str(row_end)
    request_url += '/' + output_ext
    return request_url


def get_row_count(table, report_year):
    '''Input specific table name, returns number of rows from API as XML
    then converts to integer'''
    count_url = _config['enviro_url'] + table
    if report_year != '': count_url += '/REPORTING_YEAR/=/' + report_year
    count_url += '/COUNT'
    while True:
        try:
            count_request = requests.get(count_url)
            count_xml = minidom.parseString(count_request.text)
            table_count = count_xml.getElementsByTagName('RequestRecordCount')
            table_count = int(table_count[0].firstChild.nodeValue)
            break
        except: pass
    return table_count


def download_chunks(table, table_count, row_start=0, report_year='',
                    output_ext='csv', filepath=''):
    ''' docstring '''
    # Generate URL for each 10,000 row grouping and add to DataFrame
    output_table = pd.DataFrame()
    while row_start <= table_count:
        row_end = row_start + 9999
        table_url = generate_url(table=table, report_year=report_year,
                                 row_start=row_start, row_end=row_end,
                                 output_ext=output_ext)
        log.debug('url: %s', table_url)
        try:
            table_temp, temp_time = import_table(table_url, get_time=True)
        except pd.errors.ParserError:
            log.error('error in downloading table %s', table)
            return None
        output_table = pd.concat([output_table, table_temp])
        row_start += 10000
    ghgrp_metadata['time_meta'].append(temp_time)
    ghgrp_metadata['url_meta'].append(generate_url(table, 
                                         report_year=report_year, 
                                         row_start='', 
                                         output_ext='CSV'))
    ghgrp_metadata['type_meta'].append('Database')
    ghgrp_metadata['filename_meta'].append(filepath)
    if filepath: output_table.to_csv(filepath, index=False)
    return output_table


def get_facilities(facilities_file):
    """
    Loads GHGRP data by facility from the network.
    Parses data to create dataframe of GHGRP facilities along with identifying
    information such as address, zip code, lat and long.
    """    
    # initialize destination dataframe
    facilities_df = pd.DataFrame()
    # load .xlsx file from filepath
    facilities_dict = import_table(facilities_file, skip_lines=3)
    # drop excel worksheets that we do not need
    facilities_dict = drop_excel_sheets(facilities_dict,
                                        drop_sheets=['Industry Type',
                                                     'FAQs about this Data'])
    
    # for all remaining worksheets, concatenate into single dataframe
    # certain columns need to be renamed for consistency
    
    col_dict = {'Reported Address':'Address',
                'Reported City':'City',
                'Reported County':'County',
                'Reported Latitude':'Latitude',
                'Reported Longitude':'Longitude',
                'Reported State':'State',
                #'State where Emissions Occur':'State',
                'Reported Zip Code':'Zip Code',
                }
    
    for s in facilities_dict.keys():
        for k in col_dict.keys():
            if k in facilities_dict[s]:
                facilities_dict[s].rename(columns={k:col_dict[k]},
                                          inplace=True)
        facilities_df = pd.concat([facilities_df,
                                   facilities_dict[s]]).reset_index(drop=True)

    # rename certain columns
    facilities_df = facilities_df.rename(columns={'Facility Id': 'FacilityID',
                                                  'Primary NAICS Code': 'NAICS',
                                                  'Facility Name': 'FacilityName',
                                                  'Zip Code': 'Zip'})
    # keep only those columns we are interested in retaining
    fac_columns = [c for c in facility_fields.keys() if c in facilities_df]
    facilities_df = facilities_df[fac_columns]

    # drop any duplicates
    facilities_df.drop_duplicates(inplace=True)
    
    return facilities_df


def download_excel_tables():
    # define required tables for download
    required_tables = [[data_summaries_path,
                        _config['url']+_config['data_summaries_url'],
                        'Zip File'], 
                       [esbb_subparts_path,
                        _config['url']+_config['esbb_subparts_url'],
                        'Static File'],
                       [lo_subparts_path,
                        _config['url']+_config['lo_subparts_url'],
                        'Static File'],
                       ]
    
    # download each table from web and save locally
    for table in required_tables:
        temp_time = download_table(filepath=table[0], url=table[1], 
                                   get_time=True, zip_dir=table[0])
        # record metadata
        ghgrp_metadata['time_meta'].append(temp_time)
        ghgrp_metadata['url_meta'].append(table[1])
        ghgrp_metadata['type_meta'].append(table[2])
        ghgrp_metadata['filename_meta'].append(table[0])

def import_or_download_table(filepath, table, year):
    # if data already exists on local network, import the data
    if os.path.exists(filepath):
        log.info('Importing data from %s', table)
        table_df, creation_time = import_table(filepath, get_time=True)
        ghgrp_metadata['time_meta'].append(creation_time)
        ghgrp_metadata['filename_meta'].append(filepath)
        ghgrp_metadata['type_meta'].append('Database')
        ghgrp_metadata['url_meta'].append(generate_url(table, 
                                     report_year=year, 
                                     row_start='', 
                                     output_ext='CSV'))
    
    # otherwise, download the data and save to the network
    else:
        # determine number of rows in subpart emissions table
        row_count = get_row_count(table, report_year=year)
        log.info('Downloading %s (rows: %i)', table, row_count)
        # download data in chunks
        table_df = download_chunks(table=table, table_count=row_count,
                                   report_year=year, filepath=filepath)
    
    if table_df is None:
        return None
    
    # for all columns in the temporary dataframe, remove subpart-specific prefixes
    for col in table_df:
        table_df.rename(columns={col : col[len(table) + 1:]}, inplace=True)
        
    # drop any unnamed columns
    if ('unnamed' in table_df.columns[len(table_df.columns) - 1].lower()
        ) or (table_df.columns[len(table_df.columns) - 1] == ''):
        table_df.drop(table_df.columns[len(table_df.columns) - 1],
                      axis=1, inplace=True)
        
    return table_df

def download_and_parse_subpart_tables(year):
    """
    Generates a list of required subpart tables, based on report year.
    Downloads all subpart tables in the list and saves them to local network.
    Parses subpart tables to standardized EPA format, and concatenates into
    master dataframe.
    """ 
    # import list of all ghgrp tables
    ghgrp_tables_df = import_table(ghgrp_data_dir +\
                                   'all_ghgrp_tables_years.csv').fillna('')
    # filter to obtain only those tables included in the report year
    year_tables = ghgrp_tables_df[ghgrp_tables_df['REPORTING_YEAR'
                                                  ].str.contains(year)]
    # filter to obtain only those tables that include primary emissions
    year_tables = year_tables[year_tables['PrimaryEmissions'] == 1
                              ].reset_index(drop=True)
    
    # data directory where subpart emissions tables will be stored
    tables_dir = ghgrp_external_dir + 'tables/' + year + '/'
    log.info('downloading and processing GHGRP data to %s', tables_dir)
    
    # if directory does not already exist, create it
    create_paths_if_missing(tables_dir)
        
    # initialize dataframe
    ghgrp1 = pd.DataFrame(columns=ghg_cols)
    
    # for all subpart emissions tables listed...
    for subpart_emissions_table in year_tables['TABLE']:
        
        # define filepath where subpart emissions table will be stored
        filepath = tables_dir + subpart_emissions_table + '.csv'
        
        table_df = import_or_download_table(filepath, subpart_emissions_table,
                                            year)
        
        if table_df is None:
            continue
        # add 1-2 letter subpart abbreviation
        table_df['SUBPART_NAME'] = list(year_tables.loc[
            year_tables['TABLE'] == subpart_emissions_table, 'SUBPART'])[0]

        # concatenate temporary dataframe to master ghgrp1 dataframe
        ghgrp1 = pd.concat([ghgrp1, table_df])
    
    ghgrp1.reset_index(drop=True, inplace=True)       

    if 'C' in ghgrp1.SUBPART_NAME.unique():
        ghgrp1 = calculate_combustion_emissions(ghgrp1)
        # add these new columns to the list of 'group' columns
        expanded_group_cols = group_cols + ['c_co2', 'c_co2_b', 'c_ch4', 'c_n2o']
    else:
        expanded_group_cols = group_cols
    
    # combine all GHG name columns from different tables into one
    ghgrp1['Flow Description'] = ghgrp1[name_cols].fillna('').sum(axis=1)
    
    # use alias if it exists and flow is Other
    alias = [c for c in ghgrp1.columns if c in alias_cols]
    for col in alias:
        mask = ((ghgrp1['Flow Description'] == 'Other') & ~(ghgrp1[col].isna()))
        ghgrp1.loc[mask, 'Flow Description'] = ghgrp1[col]
    
    # combine all GHG quantity columns from different tables into one
    ghgrp1['FlowAmount'] = ghgrp1[quantity_cols].astype('float').fillna(0).sum(axis=1)
    # combine all method equation columns from different tables into one
    ghgrp1['METHOD'] = ghgrp1[method_cols].fillna('').sum(axis=1)
    
    ## split dataframe into two separate dataframes based on flow description
    # if flow description has been populated:
    ghgrp1a = ghgrp1.loc[ghgrp1['Flow Description'] != ''].reset_index(drop=True)
    # if flow description is blank:
    ghgrp1b = ghgrp1.loc[ghgrp1['Flow Description'] == ''].reset_index(drop=True)
    
    ## parse data where flow description has been populated (ghgrp1a)
    # keep only the necessary columns; drop all others
    ghgrp1a.drop(ghgrp1a.columns.difference(base_cols + 
                                            ['Flow Description',
                                             'FlowAmount',
                                             'METHOD',
                                             'SUBPART_NAME']),
                 1, inplace=True)
    
    ## parse data where flow description is blank (ghgrp1b)
    # keep only the necessary columns; drop all others
    ghgrp1b.drop(ghgrp1b.columns.difference(
        base_cols + expanded_group_cols + 
        ['METHOD', 'SUBPART_NAME', 'UNIT_NAME', 'FUEL_TYPE']),
        1, inplace=True)
    # 'unpivot' data to create separate line items for each group column
    ghgrp1b = ghgrp1b.melt(id_vars = base_cols + ['METHOD', 'SUBPART_NAME',
                                                  'UNIT_NAME', 'FUEL_TYPE'], 
                           var_name = 'Flow Description', 
                           value_name = 'FlowAmount')

    # combine data for same generating unit and fuel type
    ghgrp1b['UNIT_NAME'] = ghgrp1b['UNIT_NAME'].fillna('tmp')
    ghgrp1b['FUEL_TYPE'] = ghgrp1b['FUEL_TYPE'].fillna('tmp')
    ghgrp1b = ghgrp1b.groupby(['FACILITY_ID','REPORTING_YEAR',
                               'SUBPART_NAME','UNIT_NAME','FUEL_TYPE',
                               'Flow Description'])\
        .agg({'FlowAmount':['sum'], 'METHOD':['sum']})
    ghgrp1b = ghgrp1b.reset_index()
    ghgrp1b.columns = ghgrp1b.columns.droplevel(level=1)
    ghgrp1b.drop(['UNIT_NAME','FUEL_TYPE'], axis=1, inplace=True)

    # re-join split dataframes
    ghgrp1 = pd.concat([ghgrp1a, ghgrp1b]).reset_index(drop=True)
    
    # drop those rows where flow amount is confidential
    ghgrp1 = ghgrp1[ghgrp1['FlowAmount'] != 'confidential']

    return ghgrp1


def calculate_combustion_emissions(df):
    """For subpart C, calculate total stationary fuel combustion emissions by GHG. 
    emissions are calculated as the sum of four methodological alternatives for
    calculating emissions from combustion (Tier 1-4), plus an alternative to any
    of the four tiers for units that report year-round heat input data to EPA (Part 75)
    """
    df[subpart_c_cols] = df[subpart_c_cols].replace(np.nan, 0.0)
    # nonbiogenic carbon:
        # NOTE: 'PART_75_CO2_EMISSIONS_METHOD' includes biogenic carbon emissions, 
        # so there will be a slight error here, but biogenic/nonbiogenic emissions 
        # for Part 75 are not reported separately.
    df['c_co2'] = df['TIER1_CO2_COMBUSTION_EMISSIONS'] + \
                      df['TIER2_CO2_COMBUSTION_EMISSIONS'] + \
                      df['TIER3_CO2_COMBUSTION_EMISSIONS'] + \
                      df['TIER_123_SORBENT_CO2_EMISSIONS'] + \
                      df['TIER_4_TOTAL_CO2_EMISSIONS'] - \
                      df['TIER_4_BIOGENIC_CO2_EMISSIONS'] + \
                      df['PART_75_CO2_EMISSIONS_METHOD'] -\
                      df['TIER123_BIOGENIC_CO2_EMISSIONS']              
    # biogenic carbon:
    df['c_co2_b'] = df['TIER123_BIOGENIC_CO2_EMISSIONS'] + \
                        df['TIER_4_BIOGENIC_CO2_EMISSIONS']
    # methane:    
    df['c_ch4'] = df['TIER1_CH4_COMBUSTION_EMISSIONS'] + \
                      df['TIER2_CH4_COMBUSTION_EMISSIONS'] + \
                      df['TIER3_CH4_COMBUSTION_EMISSIONS'] + \
                      df['T4CH4COMBUSTIONEMISSIONS'] + \
                      df['PART_75_CH4_EMISSIONS_CO2E']/CH4GWP
    # nitrous oxide:
    df['c_n2o'] = df['TIER1_N2O_COMBUSTION_EMISSIONS'] + \
                      df['TIER2_N2O_COMBUSTION_EMISSIONS'] + \
                      df['TIER3_N2O_COMBUSTION_EMISSIONS'] + \
                      df['T4N2OCOMBUSTIONEMISSIONS'] + \
                      df['PART_75_N2O_EMISSIONS_CO2E']/N2OGWP
      
    # drop subpart C columns because they are no longer needed
    df.drop(subpart_c_cols, axis=1, inplace=True)
    return df


def parse_additional_suparts_data(addtnl_subparts_path, subpart_cols_file, year):
    log.info('loading additional subpart data from %s', addtnl_subparts_path)
    # load .xslx data for additional subparts from filepath
    addtnl_subparts_dict = import_table(addtnl_subparts_path)
    # import column headers data for additional subparts
    subpart_cols = import_table(ghgrp_data_dir + subpart_cols_file)
    # get list of tabs to process
    addtnl_tabs = subpart_cols['tab_name'].unique()
    for key, df in list(addtnl_subparts_dict.items()):
        if key in addtnl_tabs:
            for column in df:
                df.rename(columns={column: column.replace('\n',' ')}, inplace=True)
                df.rename(columns={'Facility ID' : 'GHGRP ID',
                                   'Reporting Year' : 'Year'}, inplace=True)
            addtnl_subparts_dict[key] = df
        else:
            del addtnl_subparts_dict[key]
    # initialize dataframe
    ghgrp = pd.DataFrame()
    addtnl_base_cols = ['GHGRP ID', 'Year']
    
    # for each of the tabs in the excel workbook...
    for tab in addtnl_tabs:
        cols = subpart_cols[subpart_cols['tab_name'] == tab].reset_index(drop=True)
        col_dict = {}
        
        for i in cols['column_type'].unique():
            col_dict[i] = list(cols.loc[cols['column_type'] == i]['column_name'])

        # create temporary dataframe from worksheet, using just the desired columns      
        subpart_df = addtnl_subparts_dict[tab][
            addtnl_base_cols + list(set().union(*col_dict.values()))]
        # keep only those data for the specified report year
        subpart_df = subpart_df[subpart_df['Year'] == int(year)]
        
        if 'method' in col_dict.keys():
            # combine all method equation columns into one, drop old method columns
            subpart_df['METHOD'] = subpart_df[col_dict['method']
                                              ].fillna('').sum(axis=1)
            subpart_df.drop(col_dict['method'], axis=1, inplace=True)
        else:
            subpart_df['METHOD'] = ''

        if 'flow' in col_dict.keys():
            n = len(col_dict['flow'])
            i = 1
            subpart_df.rename(columns={col_dict['flow'][0]:'Flow Name'},
                              inplace=True)
            while i<n:
                subpart_df['Flow Name'].fillna(subpart_df[col_dict['flow'][i]],
                                               inplace=True)
                del subpart_df[col_dict['flow'][i]]
                i+= 1
        fields = []
        fields = [c for c in subpart_df.columns if c in ['METHOD','Flow Name']]
       
        # 'unpivot' data to create separate line items for each quantity column
        temp_df = subpart_df.melt(id_vars = addtnl_base_cols + fields, 
                               var_name = 'Flow Description', 
                               value_name = 'FlowAmount')
        
        # drop those rows where flow amount is confidential
        temp_df = temp_df[temp_df['FlowAmount'] != 'confidential']
        
        # add 1-2 letter subpart abbreviation
        temp_df['SUBPART_NAME'] = cols['subpart_abbr'][0]
        
        # concatentate temporary dataframe with master dataframe
        ghgrp = pd.concat([ghgrp, temp_df])
            
    # drop those rows where flow amount is negative, zero, or NaN
    ghgrp = ghgrp[ghgrp['FlowAmount'] > 0]
    ghgrp = ghgrp[ghgrp['FlowAmount'].notna()]
    
    ghgrp = ghgrp.rename(columns={'GHGRP ID': 'FACILITY_ID',
                                  'Year': 'REPORTING_YEAR'})
    
    return ghgrp


def parse_subpart_O(year):
    # parse emissions data for subpart O
    df = parse_additional_suparts_data(lo_subparts_path,
                                       'o_subparts_columns.csv', year)
    # convert subpart O data from CO2e to mass of HFC23 emitted,
    # maintain CO2e for validation
    df['AmountCO2e'] = df['FlowAmount']*1000
    df.loc[df['SUBPART_NAME'] == 'O', 'FlowAmount'] =\
        df['FlowAmount']/HFC23GWP
    df.loc[df['SUBPART_NAME'] == 'O', 'Flow Description'] =\
        'Total Reported Emissions Under Subpart O (metric tons HFC-23)'
    return df


def parse_subpart_L(year):
    # parse emissions data for subpart L
    df = parse_additional_suparts_data(lo_subparts_path,
                                       'l_subparts_columns.csv', year)
    subpart_L_GWPs = load_subpart_l_gwp()
    df = df.merge(subpart_L_GWPs, how='left', on=['Flow Name','Flow Description'])
    df['CO2e_factor'] = df['CO2e_factor'].fillna(1)
    # drop old Flow Description column
    df.drop(columns=['Flow Description'], inplace=True)
    # Flow Name column becomes new Flow Description
    df.rename(columns={'Flow Name' : 'Flow Description'}, inplace=True)
    # calculate mass flow amount based on emissions in CO2e and GWP
    df['AmountCO2e'] = df['FlowAmount']*1000
    df['FlowAmount (mass)'] = df['FlowAmount'] / df['CO2e_factor']
    # drop unnecessary columns
    df.drop(columns=['FlowAmount', 'CO2e_factor'], inplace=True)
    # rename Flow Amount column
    df.rename(columns={'FlowAmount (mass)' : 'FlowAmount'}, inplace=True)
    return df


def generate_national_totals_validation(validation_table, year):
    # define filepath for reference data
    ref_filepath = ghgrp_external_dir + 'GHGRP_reference.csv'
    
    reference_df = import_or_download_table(ref_filepath, validation_table, year)
                
    # parse reference dataframe to prepare it for validation
    reference_df['YEAR'] = reference_df['YEAR'].astype('str')
    reference_df = reference_df[reference_df['YEAR'] == year]
    reference_df.reset_index(drop=True, inplace=True)
    reference_df['FlowAmount'] = reference_df['GHG_QUANTITY'].astype(float) * 1000
    # Maintain some flows in CO2e for validation
    reference_df.loc[reference_df['GAS_CODE'].isin(flows_CO2e), 
                                  'FlowAmount'] =\
        reference_df['CO2E_EMISSION'].astype(float) * 1000
    reference_df.loc[reference_df['GAS_CODE'].isin(flows_CO2e), 
                                  'GAS_NAME'] =\
        reference_df['GAS_NAME'] + ' (CO2e)'

    reference_df = reference_df[['FlowAmount', 'GAS_NAME', 'GAS_CODE',
                                 'FACILITY_ID', 'SUBPART_NAME']]
    reference_df.rename(columns={'FACILITY_ID': 'FacilityID',
                                 'GAS_NAME': 'FlowName',
                                 'GAS_CODE':'FlowCode'}, inplace=True)
    reference_df_agg = reference_df.groupby(['FlowName',
                                             'FlowCode','SUBPART_NAME']
                                            ).agg({'FlowAmount': ['sum']})
    reference_df_agg.reset_index(inplace=True)
    reference_df_agg.columns = reference_df_agg.columns.droplevel(level=1)
    # save reference dataframe to network
    reference_df_agg.to_csv(data_dir + 'GHGRP_'+ year + '_NationalTotals.csv',
                            index=False)    

    # Update validationSets_Sources.csv
    date_created = time.strptime(time.ctime(os.path.getctime(ref_filepath)))
    date_created = time.strftime('%d-%b-%Y', date_created)
    validation_dict = {'Inventory':'GHGRP',
                       #'Version':'',
                       'Year':year,
                       'Name':'GHGRP Table V_GHG_EMITTER_SUBPART',
                       'URL': generate_url(validation_table, report_year = '',
                       row_start='', output_ext='CSV'),
                       'Criteria':'',
                       'Date Acquired':date_created,
                       }
    update_validationsets_sources(validation_dict, date_acquired=True)

def validate_national_totals_by_subpart(tab_df, year):
    log.info('validating flowbyfacility against national totals')

    # apply CO2e factors for some flows
    mask = (tab_df['AmountCO2e'].isna() & tab_df['FlowID'].isin(flows_CO2e))
    tab_df.loc[mask, 'Flow Description'] = 'Fluorinated GHG Emissions (mt CO2e)'
    subpart_L_GWPs = load_subpart_l_gwp()
    subpart_L_GWPs.rename(columns={'Flow Name':'FlowName'}, inplace=True)
    tab_df = tab_df.merge(subpart_L_GWPs, how='left',
                          on=['FlowName','Flow Description'])
    tab_df['CO2e_factor'] = tab_df['CO2e_factor'].fillna(1)
    tab_df.loc[mask, 'AmountCO2e'] = tab_df['FlowAmount']*tab_df['CO2e_factor']
    
    # for subset of flows, use CO2e for validation
    mask = tab_df['FlowID'].isin(flows_CO2e)
    tab_df.loc[mask, 'FlowAmount'] = tab_df['AmountCO2e']
    
    # parse tabulated data            
    tab_df.drop(['FacilityID','DataReliability','FlowName'], axis=1, inplace=True)
    tab_df.rename(columns={'Process': 'SubpartName',
                           'FlowID':'FlowName'}, inplace=True)
    
    # import and parse reference data
    ref_df = import_table(data_dir + 'GHGRP_'+ year + '_NationalTotals.csv')
    ref_df.drop(['FlowName'], axis=1, inplace=True)
    ref_df.rename(columns={'SUBPART_NAME': 'SubpartName',
                           'FlowCode':'FlowName'}, inplace=True)
    
    validation_result = validate_inventory(tab_df, ref_df, group_by='subpart')
    # Update flow names to indicate which are in CO2e
    validation_result.loc[validation_result['FlowName'].isin(flows_CO2e),
                          'FlowName'] = validation_result['FlowName']+' (CO2e)'
    write_validation_result('GHGRP', year, validation_result)
    

def generate_metadata(year, metadata_dict, datatype = 'inventory'):
    """
    Gets metadata and writes to .json
    """
    if datatype == 'source':
        source_path = metadata_dict['filename_meta']
        source_meta = compile_source_metadata(source_path, _config, year)
        source_meta['SourceType'] = metadata_dict['type_meta']
        source_meta['SourceURL'] = metadata_dict['url_meta']
        source_meta['SourceAcquisitionTime'] = metadata_dict['time_meta']
        write_metadata('GHGRP_'+year, source_meta,
                       category=ext_folder, datatype='source')
    else:
        source_meta = read_source_metadata(paths, set_stewi_meta('GHGRP_' + year,
                                           ext_folder),
                                           force_JSON=True)['tool_meta']
        write_metadata('GHGRP_'+year, source_meta, datatype=datatype)

def load_subpart_l_gwp():
    
    # load global warming potentials for subpart L calculation
    subpart_L_GWPs_url = _config['subpart_L_GWPs_url']
    filepath = ghgrp_external_dir + 'Subpart L Calculation Spreadsheet.xls' 
    if not(os.path.exists(filepath)):
        download_table(filepath = filepath, url = subpart_L_GWPs_url)
    table1 = pd.read_excel(filepath, sheet_name = 'Lookup Tables',
                                   usecols = "A,D")
    table1.rename(columns={'Global warming potential (100 yr.)':'CO2e_factor',
                           'Name':'Flow Name'},
                  inplace = True)
    # replace emdash with hyphen
    table1['Flow Name'] = table1['Flow Name'].str.replace('–', '-')
    table2 = pd.read_excel(filepath, sheet_name = 'Lookup Tables',
                                   usecols = "G,H", nrows=12)
    table2.rename(columns={'Default Global Warming Potential':'CO2e_factor',
                           'Fluorinated GHG Groupd':'Flow Name'},
              inplace = True)

    # rename certain fluorinated GHG groups for consistency
    table2['Flow Name'] = table2['Flow Name'].str.replace(
        'Saturated HFCs with 2 or fewer carbon-hydrogen bonds',
        'Saturated hydrofluorocarbons (HFCs) with 2 or fewer '
        'carbon-hydrogen bonds')
    table2['Flow Name'] = table2['Flow Name'].str.replace(
        'Saturated HFEs and HCFEs with 1 carbon-hydrogen bond',
        'Saturated hydrofluoroethers (HFEs) and hydrochlorofluoroethers '
        '(HCFEs) with 1 carbon-hydrogen bond')
    table2['Flow Name'] = table2['Flow Name'].str.replace(
        'Unsaturated PFCs, unsaturated HFCs, unsaturated HCFCs, '
        'unsaturated halogenated ethers, unsaturated halogenated '
        'esters, fluorinated aldehydes, and fluorinated ketones',
        'Unsaturated perfluorocarbons (PFCs), unsaturated HFCs, '
        'unsaturated hydrochlorofluorocarbons (HCFCs), unsaturated '
        'halogenated ethers, unsaturated halogenated esters, '
        'fluorinated aldehydes, and fluorinated ketones')

    subpart_L_GWPs = pd.concat([table1, table2])
    subpart_L_GWPs['Flow Description'] = 'Fluorinated GHG Emissions (mt CO2e)'
    return subpart_L_GWPs


def main(**kwargs):
    
    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Download and save GHGRP data\
                        [B] Generate inventory files for StEWI and validate\
                        [C] Download national totals data for validation',
                        type = str)

    parser.add_argument('-Y', '--Year', nargs = '+',
                        help = 'What GHGRP year do you want to retrieve',
                        type = str)
    
    if len(kwargs) == 0:
        kwargs = vars(parser.parse_args())
    
    for year in kwargs['Year']:
        pickle_file = ghgrp_external_dir + 'GHGRP_' + year + '.pk'
        if kwargs['Option'] == 'A':
            
            download_excel_tables()
 
            # download subpart emissions tables for report year and save locally
            # parse subpart emissions data to match standardized EPA format
            ghgrp1 = download_and_parse_subpart_tables(year)

            # parse emissions data for subparts E, BB, CC, LL (S already accounted for)
            ghgrp2 = parse_additional_suparts_data(esbb_subparts_path,
                                                   'esbb_subparts_columns.csv', year) 
        
            # parse emissions data for subpart O
            ghgrp3 = parse_subpart_O(year)
   
            # parse emissions data for subpart L
            ghgrp4 = parse_subpart_L(year)
                            
            # concatenate ghgrp1, ghgrp2, ghgrp3, and ghgrp4
            ghgrp = pd.concat([ghgrp1, ghgrp2,
                               ghgrp3, ghgrp4]).reset_index(drop=True)
                        
            # map flow descriptions to standard gas names from GHGRP
            ghg_mapping = pd.read_csv(ghgrp_data_dir + 'ghg_mapping.csv',
                                      usecols=['Flow Description', 'FlowName',
                                               'GAS_CODE'])
            ghgrp = pd.merge(ghgrp, ghg_mapping, on='Flow Description',
                             how='left')
            missing = ghgrp[ghgrp['FlowName'].isna()]
            if len(missing)>0:
                log.warning('some flows are unmapped')
            ghgrp.drop('Flow Description', axis=1, inplace=True)
            
            # rename certain columns for consistency
            ghgrp.rename(columns={'FACILITY_ID':'FacilityID',
                                  'NAICS_CODE':'NAICS',
                                  'GAS_CODE':'FlowCode'}, inplace=True)    
            
            # pickle data and save to network
            log.info('saving processed GHGRP data to %s', pickle_file)
            ghgrp.to_pickle(pickle_file)
            
            #Metadata
            generate_metadata(year, ghgrp_metadata, datatype='source')

        if kwargs['Option'] == 'B':
            log.info('extracting data from %s', pickle_file)
            ghgrp = pd.read_pickle(pickle_file)
            
            # import data reliability scores 
            ghgrp_reliability_table = get_reliability_table_for_source('GHGRPa')
            
            # add reliability scores
            ghgrp = pd.merge(ghgrp, ghgrp_reliability_table,
                             left_on='METHOD',
                             right_on='Code', how='left')
                        
            # fill NAs with 5 for DQI reliability score
            ghgrp['DQI Reliability Score'] = ghgrp['DQI Reliability Score'
                                                   ].fillna(value=5)
                       
            # convert metric tons to kilograms
            ghgrp['FlowAmount'] = 1000 * ghgrp['FlowAmount'].astype('float')
            
            # rename reliability score column for consistency
            ghgrp.rename(columns={'DQI Reliability Score': 'DataReliability',
                                  'SUBPART_NAME':'Process',
                                  'FlowCode':'FlowID'}, inplace=True)
            ghgrp['ProcessType'] = 'Subpart'
            
            log.info('generating flowbysubpart output')

            # generate flowbysubpart
            fbs_columns = [c for c in flowbyprocess_fields.keys() if c in ghgrp]
            ghgrp_fbs = ghgrp[fbs_columns]
            ghgrp_fbs = aggregate(ghgrp_fbs, ['FacilityID', 'FlowName', 'Process',
                                              'ProcessType'])
            store_inventory(ghgrp_fbs,'GHGRP_'+year,'flowbyprocess')
            
            log.info('generating flowbyfacility output')
            fbf_columns = [c for c in flowbyfacility_fields.keys() if c in ghgrp]
            ghgrp_fbf = ghgrp[fbf_columns]
            
            # aggregate instances of more than one flow for same facility and flow type
            ghgrp_fbf_2 = aggregate(ghgrp_fbf, ['FacilityID', 'FlowName'])
            store_inventory(ghgrp_fbf_2,'GHGRP_'+year,'flowbyfacility')
        
            log.info('generating flows output')
            flow_columns = ['FlowName', 'FlowID']
            ghgrp_flow = ghgrp[flow_columns].drop_duplicates()
            ghgrp_flow.dropna(subset=['FlowName'], inplace=True)
            ghgrp_flow.sort_values(by=['FlowID','FlowName'], inplace=True)
            ghgrp_flow['Compartment'] = 'air'
            ghgrp_flow['Unit'] = 'kg'
            store_inventory(ghgrp_flow,'GHGRP_'+year,'flow')
        
            log.info('generating facilities output')
            # return dataframe of GHGRP facilities
            facilities_df = get_facilities(data_summaries_path +\
                                           'ghgp_data_' + year + '.xlsx')
            
            # add facility information based on facility ID
            ghgrp = ghgrp.merge(facilities_df, on='FacilityID', how='left')

            # generate facilities output and save to network
            facility_columns = [c for c in facility_fields.keys() if c in ghgrp]
            ghgrp_facility = ghgrp[facility_columns].drop_duplicates()
            ghgrp_facility.dropna(subset=['FacilityName'], inplace=True)
            # ensure NAICS does not have trailing decimal/zero
            ghgrp_facility['NAICS'] = ghgrp_facility['NAICS'].fillna(0)
            ghgrp_facility['NAICS'] = ghgrp_facility['NAICS'].astype(int).astype(str)
            ghgrp_facility.loc[ghgrp_facility['NAICS']=='0','NAICS'] = None
            ghgrp_facility.sort_values(by=['FacilityID'], inplace=True)
            store_inventory(ghgrp_facility,'GHGRP_'+year,'facility')
            
            validate_national_totals_by_subpart(ghgrp, year)
            
            # Record metadata compiled from all GHGRP files and tables
            generate_metadata(year, ghgrp_metadata, datatype='inventory')
      
        elif kwargs['Option'] == 'C':
            log.info('generating national totals for validation')
            validation_table = 'V_GHG_EMITTER_SUBPART'
            generate_national_totals_validation(validation_table, year)

if __name__ == '__main__':
    main()
