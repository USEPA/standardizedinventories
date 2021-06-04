#!/usr/bin/env python
# coding=utf-8

"""
Imports GHGRP data and processes to Standardized EPA output format.
This file requires parameters be passed like:

    Option -y Year 

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
    2018
    2017
    2016
    2015   
    2014
    2013
    2012
    
Models with tables available at https://www.epa.gov/enviro/greenhouse-gas-model
Envirofacts web services documentation can be found at: https://www.epa.gov/enviro/web-services
"""

from stewi.globals import set_dir, download_table, inventory_metadata,\
    write_metadata, get_relpath, import_table, drop_excel_sheets,\
    validate_inventory, validation_summary, write_validation_result,\
    weighted_average, data_dir, output_dir, reliability_table,\
    flowbyfacility_fields, flowbySCC_fields, facility_fields, config
import pandas as pd
import numpy as np
import requests
from xml.dom import minidom
import os
from datetime import datetime
import argparse
import logging as log


_config = config()['databases']['GHGRP']
## define directories
data_dir = data_dir # stewi data directory
output_dir = output_dir # stewi output directory
ghgrp_data_dir = set_dir(data_dir + 'ghgrp/') # stewi data directory --> ghgrp
ghgrp_external_dir = set_dir(data_dir + '/../../../GHGRP Data Files/') # external GHGRP data directory
   
# Flow codes that are reported in validation in CO2e
flows_CO2e = ['PFC', 'HFC', 'Other','Very_Short', 'HFE', 'Other_Full']


def generate_url(table, report_year='', row_start=0, row_end=9999, output_ext='JSON'):
    # Input a specific table name to generate the query URL to submit
    request_url = _config['enviro_url'] + table
    if report_year != '': request_url += '/REPORTING_YEAR/=/' + report_year
    if row_start != '': request_url += '/ROWS/' + str(row_start) + ':' + str(row_end)
    request_url += '/' + output_ext
    return request_url


def get_row_count(table, report_year):
    # Input specific table name, returns number of rows from API as XML then converts to integer
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


def download_chunks(table, table_count, row_start=0, report_year='', output_ext='JSON', filepath=''):
    # Generate URL for each 10,000 row grouping and add to DataFrame
    output_table = pd.DataFrame()
    while row_start <= table_count:
        row_end = row_start + 9999
        table_url = generate_url(table=table, report_year=report_year, row_start=row_start, row_end=row_end,
                                 output_ext='csv')
        print('url: ' + table_url)
        while True:
            try:
                table_temp, temp_time = import_table(table_url, get_time=True)
                time_meta.append(temp_time)
                url_meta.append(table_url)
                type_meta.append('Database')
                filename_meta.append(get_relpath(filepath))
                break
            except ValueError: continue
            except: break
        output_table = pd.concat([output_table, table_temp])
        row_start += 10000
    output_table.drop_duplicates(inplace=True)
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
    facilities_dict = drop_excel_sheets(facilities_dict, drop_sheets=['Industry Type', 'FAQs about this Data'])
    
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
                facilities_dict[s].rename(columns={k:col_dict[k]}, inplace=True)
        facilities_df = pd.concat([facilities_df, facilities_dict[s]]).reset_index(drop=True)

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


def download_and_parse_subpart_tables(year):
    """
    Generates a list of required subpart tables, based on report year.
    Downloads all subpart tables in the list and saves them to local network.
    Parses subpart tables to standardized EPA format, and concatenates into
    master dataframe.
    """ 
    
    # import list of all ghgrp tables
    ghgrp_tables_df = import_table(ghgrp_data_dir + 'all_ghgrp_tables_years.csv').fillna('')
    # filter to obtain only those tables included in the report year
    year_tables = ghgrp_tables_df[ghgrp_tables_df['REPORTING_YEAR'].str.contains(year)]
    # filter to obtain only those tables that include primary emissions
    year_tables = year_tables[year_tables['PrimaryEmissions'] == 1].reset_index(drop=True)
    
    # data directory where subpart emissions tables will be stored
    tables_dir = ghgrp_external_dir + 'tables/' + year + '/'
    
    # if directory does not already exist, create it
    if not os.path.exists(tables_dir):
        os.makedirs(tables_dir)
        
    # initialize dataframe
    ghgrp1 = pd.DataFrame(columns=ghg_cols)
    
    # for all subpart emissions tables listed...
    for subpart_emissions_table in year_tables['TABLE']:
        
        # define filepath where subpart emissions table will be stored
        filepath = tables_dir + subpart_emissions_table + '.csv'
        
        # if data already exists on local network, import the data
        if os.path.exists(filepath):
            log.info('importing data from %s', subpart_emissions_table)
            temp_df, temp_time = import_table(filepath, get_time=True)
            table_length = len(temp_df)
            row_start = 0
            while row_start < table_length:
                time_meta.append(temp_time)
                filename_meta.append(get_relpath(filepath))
                type_meta.append('Database')
                url_meta.append(generate_url(subpart_emissions_table, 
                                             report_year=year, 
                                             row_start=row_start, 
                                             row_end=row_start + 10000, 
                                             output_ext='CSV'))
                row_start += 10000
        
        # otherwise, download the data and save to the network
        else:
            # determine number of rows in subpart emissions table
            subpart_count = get_row_count(subpart_emissions_table, report_year=year)
            log.info('Downloading ' + subpart_emissions_table + '(rows: ' + str(subpart_count) + ')')
            # download data in chunks
            while True:
                try:
                    temp_df = download_chunks(table=subpart_emissions_table, table_count=subpart_count, report_year=year, filepath=filepath)
                    log.info('Done downloading.')
                    break
                except ValueError: continue
                except: break
        
        # for all columns in the temporary dataframe, remove subpart-specific prefixes
        for col in temp_df:
            temp_df.rename(columns={col : col[len(subpart_emissions_table) + 1:]}, inplace=True)
            
        # drop any unnamed columns
        if 'unnamed' in temp_df.columns[len(temp_df.columns) - 1].lower() or temp_df.columns[len(temp_df.columns) - 1] == '':
            temp_df.drop(temp_df.columns[len(temp_df.columns) - 1], axis=1, inplace=True)
        
        # add 1-2 letter subpart abbreviation
        temp_df['SUBPART_NAME'] = list(year_tables.loc[year_tables['TABLE'] == subpart_emissions_table, 'SUBPART'])[0]

        # concatenate temporary dataframe to master ghgrp1 dataframe
        ghgrp1 = pd.concat([ghgrp1, temp_df])
    
    ghgrp1.reset_index(drop=True, inplace=True)       
    # for subpart C, calculate total stationary fuel combustion emissions by greenhouse gas 
    # emissions are calculated as the sum of four methodological alternatives for
    # calculating emissions from combustion (Tier 1-4), plus an alternative to any
    # of the four tiers for units that report year-round heat input data to EPA (Part 75)
    ghgrp1[subpart_c_cols] = ghgrp1[subpart_c_cols].replace(np.nan, 0.0)
    # nonbiogenic carbon:
        # NOTE: 'PART_75_CO2_EMISSIONS_METHOD' includes biogenic carbon emissions, 
        # so there will be a slight error here, but biogenic/nonbiogenic emissions 
        # for Part 75 are not reported separately.
    ghgrp1['c_co2'] = ghgrp1['TIER1_CO2_COMBUSTION_EMISSIONS'] + \
                      ghgrp1['TIER2_CO2_COMBUSTION_EMISSIONS'] + \
                      ghgrp1['TIER3_CO2_COMBUSTION_EMISSIONS'] + \
                      ghgrp1['TIER_123_SORBENT_CO2_EMISSIONS'] + \
                      ghgrp1['TIER_4_TOTAL_CO2_EMISSIONS'] - \
                      ghgrp1['TIER_4_BIOGENIC_CO2_EMISSIONS'] + \
                      ghgrp1['PART_75_CO2_EMISSIONS_METHOD'] -\
                      ghgrp1['TIER123_BIOGENIC_CO2_EMISSIONS']              
    # biogenic carbon:
    ghgrp1['c_co2_b'] = ghgrp1['TIER123_BIOGENIC_CO2_EMISSIONS'] + \
                        ghgrp1['TIER_4_BIOGENIC_CO2_EMISSIONS']
    # methane:    
    ghgrp1['c_ch4'] = ghgrp1['TIER1_CH4_COMBUSTION_EMISSIONS'] + \
                      ghgrp1['TIER2_CH4_COMBUSTION_EMISSIONS'] + \
                      ghgrp1['TIER3_CH4_COMBUSTION_EMISSIONS'] + \
                      ghgrp1['T4CH4COMBUSTIONEMISSIONS'] + \
                      ghgrp1['PART_75_CH4_EMISSIONS_CO2E']/CH4GWP
    # nitrous oxide:
    ghgrp1['c_n2o'] = ghgrp1['TIER1_N2O_COMBUSTION_EMISSIONS'] + \
                      ghgrp1['TIER2_N2O_COMBUSTION_EMISSIONS'] + \
                      ghgrp1['TIER3_N2O_COMBUSTION_EMISSIONS'] + \
                      ghgrp1['T4N2OCOMBUSTIONEMISSIONS'] + \
                      ghgrp1['PART_75_N2O_EMISSIONS_CO2E']/N2OGWP
      
    # add these new columns to the list of 'group' columns
    expanded_group_cols = group_cols + ['c_co2', 'c_co2_b', 'c_ch4', 'c_n2o']

    # drop subpart C columns because they are no longer needed
    ghgrp1.drop(subpart_c_cols, axis=1, inplace=True)
    
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
    ghgrp1a = ghgrp1[ghgrp1['Flow Description'] != '']
    # if flow description is blank:
    ghgrp1b = ghgrp1[ghgrp1['Flow Description'] == '']
    
    ## parse data where flow description has been populated (ghgrp1a)
    # keep only the necessary columns; drop all others
    ghgrp1a.drop(ghgrp1a.columns.difference(base_cols + ['Flow Description','FlowAmount', 'METHOD', 'SUBPART_NAME']),1, inplace=True)
    
    ## parse data where flow description is blank (ghgrp1b)
    # keep only the necessary columns; drop all others
    ghgrp1b.drop(ghgrp1b.columns.difference(base_cols + expanded_group_cols + ['METHOD', 'SUBPART_NAME', 'UNIT_NAME', 'FUEL_TYPE']),1, inplace=True)
    # 'unpivot' data to create separate line items for each group column
    ghgrp1b = ghgrp1b.melt(id_vars = base_cols + ['METHOD', 'SUBPART_NAME', 'UNIT_NAME', 'FUEL_TYPE'], 
                           var_name = 'Flow Description', 
                           value_name = 'FlowAmount')

    # combine data for same generating unit and fuel type
    ghgrp1b['UNIT_NAME'] = ghgrp1b['UNIT_NAME'].fillna('tmp')
    ghgrp1b['FUEL_TYPE'] = ghgrp1b['FUEL_TYPE'].fillna('tmp')
    ghgrp1b = ghgrp1b.groupby(['FACILITY_ID','REPORTING_YEAR','SUBPART_NAME','UNIT_NAME','FUEL_TYPE','Flow Description'])\
        .agg({'FlowAmount':['sum'], 'METHOD':['sum']})
    ghgrp1b = ghgrp1b.reset_index()
    ghgrp1b.columns = ghgrp1b.columns.droplevel(level=1)
    ghgrp1b.drop(['UNIT_NAME','FUEL_TYPE'], axis=1, inplace=True)

    # re-join split dataframes
    ghgrp1 = pd.concat([ghgrp1a, ghgrp1b]).reset_index(drop=True)
    
    # drop those rows where flow amount is confidential
    ghgrp1 = ghgrp1[ghgrp1['FlowAmount'] != 'confidential']

    return ghgrp1


def parse_additional_suparts_data(addtnl_subparts_path, subpart_cols_file, year):
    
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
        subpart_df = addtnl_subparts_dict[tab][addtnl_base_cols + list(set().union(*col_dict.values()))]
        # keep only those data for the specified report year
        subpart_df = subpart_df[subpart_df['Year'] == int(year)]
        
        if 'method' in col_dict.keys():
            # combine all method equation columns into one, drop old method columns
            subpart_df['METHOD'] = subpart_df[col_dict['method']].fillna('').sum(axis=1)
            subpart_df.drop(col_dict['method'], axis=1, inplace=True)
        else:
            subpart_df['METHOD'] = ''

        if 'flow' in col_dict.keys():
            n = len(col_dict['flow'])
            i = 1
            subpart_df.rename(columns={col_dict['flow'][0]:'Flow Name'}, inplace=True)
            while i<n:
                subpart_df['Flow Name'].fillna(subpart_df[col_dict['flow'][i]], inplace=True)
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

def aggregate(df, grouping_vars):
    df_agg = df.groupby(grouping_vars).agg({'FlowAmount': ['sum']})
    df_agg['DataReliability']=weighted_average(
        df, 'DataReliability', 'FlowAmount', grouping_vars)
    df_agg = df_agg.reset_index()
    df_agg.columns = df_agg.columns.droplevel(level=1)
    # drop those rows where flow amount is negative, zero, or NaN
    df_agg = df_agg[df_agg['FlowAmount'] > 0]
    df_agg = df_agg[df_agg['FlowAmount'].notna()]
    return df_agg

def validate_national_totals_by_subpart(tab_df, year):
    log.info('validating flowbyfacility against national totals')

    # apply CO2e factors for some flows
    mask = (tab_df['AmountCO2e'].isna() & tab_df['FlowCode'].isin(flows_CO2e))
    tab_df.loc[mask, 'Flow Description'] = 'Fluorinated GHG Emissions (mt CO2e)'
    subpart_L_GWPs = load_subpart_l_gwp()
    subpart_L_GWPs.rename(columns={'Flow Name':'FlowName'}, inplace=True)
    tab_df = tab_df.merge(subpart_L_GWPs, how='left',
                          on=['FlowName','Flow Description'])
    tab_df['CO2e_factor'] = tab_df['CO2e_factor'].fillna(1)
    tab_df.loc[mask, 'AmountCO2e'] = tab_df['FlowAmount']*tab_df['CO2e_factor']
    
    # for subset of flows, use CO2e for validation
    mask = tab_df['FlowCode'].isin(flows_CO2e)
    tab_df.loc[mask, 'FlowAmount'] = tab_df['AmountCO2e']
    
    # parse tabulated data            
    tab_df.drop(['FacilityID','DataReliability','FlowName'], axis=1, inplace=True)
    tab_df.rename(columns={'SCC': 'SubpartName',
                           'FlowCode':'FlowName'}, inplace=True)
    
    # import and parse reference data
    ref_df = import_table(ghgrp_external_dir + year + '_GHGRP_NationalTotals.csv')
    ref_df.drop(['FacilityID','FlowName'], axis=1, inplace=True)
    ref_df.rename(columns={'SUBPART_NAME': 'SubpartName',
                           'FlowCode':'FlowName'}, inplace=True)
    
    validation_result = validate_inventory(tab_df, ref_df, group_by='subpart')
    write_validation_result('GHGRP', year, validation_result)
    
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
        'Saturated hydrofluorocarbons (HFCs) with 2 or fewer carbon-hydrogen bonds')
    table2['Flow Name'] = table2['Flow Name'].str.replace(
        'Saturated HFEs and HCFEs with 1 carbon-hydrogen bond',
        'Saturated hydrofluoroethers (HFEs) and hydrochlorofluoroethers (HCFEs) with 1 carbon-hydrogen bond')
    table2['Flow Name'] = table2['Flow Name'].str.replace(
        'Unsaturated PFCs, unsaturated HFCs, unsaturated HCFCs, unsaturated halogenated ethers, unsaturated halogenated esters, fluorinated aldehydes, and fluorinated ketones',
        'Unsaturated perfluorocarbons (PFCs), unsaturated HFCs, unsaturated hydrochlorofluorocarbons (HCFCs), unsaturated halogenated ethers, unsaturated halogenated esters, fluorinated aldehydes, and fluorinated ketones')

    subpart_L_GWPs = pd.concat([table1, table2])
    subpart_L_GWPs['Flow Description'] = 'Fluorinated GHG Emissions (mt CO2e)'
    return subpart_L_GWPs

########## START HERE ###############
if __name__ == '__main__':
    
    ## parse Option and Year arguments
    
    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Download and save GHGRP data\
                        [B] Generate inventory files for StEWI and validate\
                        [C] Download national totals data for validation',
                        type = str)

    parser.add_argument('-y', '--Year', nargs = '+',
                        help = 'What GHGRP year do you want to retrieve',
                        type = str)
    
    args = parser.parse_args()
    GHGRPyears = args.Year
    
    # define GWPs
    # (these values are from IPCC's AR4, which is consistent with GHGRP methodology)
    CH4GWP = 25
    N2OGWP = 298
    HFC23GWP = 14800
    
    # define column groupings
    ghgrp_columns = import_table(ghgrp_data_dir + 'ghgrp_columns.csv')
    name_cols = list(ghgrp_columns[ghgrp_columns['ghg_name'] == 1]['column_name'])
    alias_cols = list(ghgrp_columns[ghgrp_columns['ghg_alias'] == 1]['column_name'])
    quantity_cols = list(ghgrp_columns[ghgrp_columns['ghg_quantity'] == 1]['column_name'])
    co2_cols = list(ghgrp_columns[ghgrp_columns['co2'] == 1]['column_name'])
    ch4_cols = list(ghgrp_columns[ghgrp_columns['ch4'] == 1]['column_name'])
    n2o_cols = list(ghgrp_columns[ghgrp_columns['n2o'] == 1]['column_name'])
    co2e_cols = list(ghgrp_columns[ghgrp_columns['co2e_quantity'] == 1]['column_name'])
    subpart_c_cols = list(ghgrp_columns[ghgrp_columns['subpart_c'] == 1]['column_name'])
    method_cols = list(ghgrp_columns[ghgrp_columns['method'] == 1]['column_name'])
    base_cols = list(ghgrp_columns[ghgrp_columns['base_columns'] == 1]['column_name'])
    info_cols = name_cols + quantity_cols + method_cols
    group_cols = co2_cols + ch4_cols + n2o_cols 
    ghg_cols = base_cols + info_cols + group_cols
    
    # define filepaths for downloaded data
    data_summaries_path = ghgrp_external_dir + _config['most_recent_year'] + '_data_summary_spreadsheets/'
    esbb_subparts_path = ghgrp_external_dir + _config['esbb_subparts_url']
    lo_subparts_path = ghgrp_external_dir + _config['lo_subparts_url']
    
    # set format for metadata file
    ghgrp_metadata = inventory_metadata 
    time_meta = []
    filename_meta = []
    type_meta = []
    url_meta = []
    
    for year in GHGRPyears:
        
        if args.Option == 'A':
            log.info('downloading and processing GHGRP data')
            
            # define required tables for download
            required_tables = [[data_summaries_path, _config['url']+_config['data_summaries_url'], 'Static File'], 
                               [esbb_subparts_path, _config['url']+_config['esbb_subparts_url'], 'Static File'],
                               [lo_subparts_path, _config['url']+_config['lo_subparts_url'], 'Static File'],
                               ]
            
            # download each table from web and save locally
            for table in required_tables:
                temp_time = download_table(filepath=table[0], url=table[1], 
                                           get_time=True, zip_dir=table[0])
                # record metadata
                time_meta.append(temp_time)
                url_meta.append(table[1])
                type_meta.append(table[2])
                filename_meta.append(get_relpath(table[0]))
 
            # download subpart emissions tables for report year and save locally
            # parse subpart emissions data to match standardized EPA format
            ghgrp1 = download_and_parse_subpart_tables(year)

            # parse emissions data for subparts E, BB, CC, LL (S already accounted for)
            ghgrp2 = parse_additional_suparts_data(esbb_subparts_path, 'esbb_subparts_columns.csv', year) 
        
            # parse emissions data for subpart O
            ghgrp3 = parse_additional_suparts_data(lo_subparts_path, 'o_subparts_columns.csv', year)

            # convert subpart O data from CO2e to mass of HFC23 emitted, maintain CO2e for validation
            ghgrp3['AmountCO2e'] = ghgrp3['FlowAmount']*1000
            ghgrp3.loc[ghgrp3['SUBPART_NAME'] == 'O', 'FlowAmount'] =\
                ghgrp3['FlowAmount']/HFC23GWP
            ghgrp3.loc[ghgrp3['SUBPART_NAME'] == 'O', 'Flow Description'] =\
                'Total Reported Emissions Under Subpart O (metric tons HFC-23)'
   
            # parse emissions data for subpart L
            ghgrp4 = parse_additional_suparts_data(lo_subparts_path, 'l_subparts_columns.csv', year)

            subpart_L_GWPs = load_subpart_l_gwp()
            ghgrp4 = ghgrp4.merge(subpart_L_GWPs, how='left',
                                  on=['Flow Name',
                                      'Flow Description'])
            ghgrp4['CO2e_factor'] = ghgrp4['CO2e_factor'].fillna(1)
            # drop old Flow Description column
            ghgrp4.drop(columns=['Flow Description'], inplace=True)
            # Flow Name column becomes new Flow Description
            ghgrp4.rename(columns={'Flow Name' : 'Flow Description'}, inplace=True)
            # calculate mass flow amount based on emissions in CO2e and GWP
            ghgrp4['AmountCO2e'] = ghgrp4['FlowAmount']*1000
            ghgrp4['FlowAmount (mass)'] = ghgrp4['FlowAmount'] / ghgrp4['CO2e_factor']
            # drop unnecessary columns
            ghgrp4.drop(columns=['FlowAmount', 'CO2e_factor'],
                                 inplace=True)
            # rename Flow Amount column
            ghgrp4.rename(columns={'FlowAmount (mass)' : 'FlowAmount'}, inplace=True)
                
            # concatenate ghgrp1, ghgrp2, ghgrp3, and ghgrp4
            ghgrp = pd.concat([ghgrp1, ghgrp2, ghgrp3, ghgrp4]).reset_index(drop=True)
                        
            # map flow descriptions to standard gas names from GHGRP
            ghg_mapping = pd.read_csv(ghgrp_data_dir + 'ghg_mapping.csv',
                                      usecols=['Flow Description', 'FlowName', 'GAS_CODE'])
            ghgrp = pd.merge(ghgrp, ghg_mapping, on='Flow Description', how='left')
            missing = ghgrp[ghgrp['FlowName'].isna()]
            if len(missing)>0:
                log.warning('some flows are unmapped')
            ghgrp.drop('Flow Description', axis=1, inplace=True)
            
            # rename certain columns for consistency
            ghgrp.rename(columns={'FACILITY_ID':'FacilityID',
                                  'NAICS_CODE':'NAICS',
                                  'GAS_CODE':'FlowCode'}, inplace=True)    
            
            # pickle data and save to network
            log.info('saving GHGRP data to pickle')
            ghgrp.to_pickle('work/GHGRP_' + year + '.pk')


        if args.Option == 'B':
            log.info('extracting data from GHGRP pickle')
            ghgrp = pd.read_pickle('work/GHGRP_' + year + '.pk')
            
            # import data reliability scores 
            ghgrp_reliability_table = reliability_table[reliability_table['Source'] == 'GHGRPa']
            ghgrp_reliability_table.drop('Source', axis=1, inplace=True)
            
            # add reliability scores
            ghgrp = pd.merge(ghgrp, ghgrp_reliability_table,
                             left_on='METHOD',
                             right_on='Code', how='left')
                        
            # fill NAs with 5 for DQI reliability score
            ghgrp['DQI Reliability Score'] = ghgrp['DQI Reliability Score'].fillna(value=5)
                       
            # convert metric tons to kilograms
            ghgrp['FlowAmount'] = 1000 * ghgrp['FlowAmount'].astype('float')
            
            # rename reliability score column for consistency
            # temporary assign as SCC for consistency with NEI
            ghgrp.rename(columns={'DQI Reliability Score': 'DataReliability',
                                  'SUBPART_NAME':'SCC'}, inplace=True)
            
            log.info('generating flowbysubpart output')
            
            # generate flowbysubpart
            fbs_columns = [c for c in flowbySCC_fields.keys() if c in ghgrp]
            ghgrp_fbs = ghgrp[fbs_columns]
            ghgrp_fbs = aggregate(ghgrp_fbs, ['FacilityID', 'FlowName', 'SCC'])
            ghgrp_fbs.to_csv(output_dir + 'flowbySCC/GHGRP_' + year + '.csv', index=False)
            
            log.info('generating flowbyfacility output')
            
            fbf_columns = [c for c in flowbyfacility_fields.keys() if c in ghgrp]
            ghgrp_fbf = ghgrp[fbf_columns]
            
            # aggregate instances of more than one flow for same facility and flow type
            ghgrp_fbf_2 = aggregate(ghgrp_fbf, ['FacilityID', 'FlowName'])
                      
            # save results to output directory
            ghgrp_fbf_2.to_csv(output_dir + 'flowbyfacility/GHGRP_' + year + '.csv', index=False)
        
            log.info('generating flows output')

            # generate flows output and save to network
            flow_columns = ['FlowName', 'FlowCode']
            ghgrp_flow = ghgrp[flow_columns].drop_duplicates()
            ghgrp_flow['Compartment'] = 'air'
            ghgrp_flow['Unit'] = 'kg'
            ghgrp_flow.to_csv(output_dir + 'flow/GHGRP_' + year + '.csv', index=False)
        
            log.info('generating facilities output')
            
            # return dataframe of GHGRP facilities
            facilities_df = get_facilities(data_summaries_path + 'ghgp_data_' + year + '.xlsx')
            
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
            ghgrp_facility.to_csv(output_dir + 'facility/GHGRP_' + year + '.csv', index=False)
            
            validate_national_totals_by_subpart(ghgrp, year)
            
            # Record metadata compiled from all GHGRP files and tables
            ghgrp_metadata['SourceAquisitionTime'] = time_meta
            ghgrp_metadata['SourceFileName'] = filename_meta
            ghgrp_metadata['SourceType'] = type_meta
            ghgrp_metadata['SourceURL'] = url_meta
            write_metadata('GHGRP', year, ghgrp_metadata)
      
        elif args.Option == 'C':
            log.info('downloading national totals for validation')
            
            validation_table = 'V_GHG_EMITTER_SUBPART'
            
            # define filepath for reference data
            ref_filepath = ghgrp_external_dir + 'GHGRP_reference' + year + '.csv'
            
            # if the reference file exists, load the data
            if os.path.exists(ref_filepath):
                reference_df, temp_time = import_table(ref_filepath, get_time=True)
                table_length = len(reference_df)
                row_start = 0
                while row_start < table_length:
                    time_meta.append(temp_time)
                    filename_meta.append(get_relpath(ref_filepath))
                    type_meta.append('Database')
                    url_meta.append(generate_url(validation_table, report_year=year, row_start=row_start, row_end=row_start + 10000, output_ext='CSV'))
                    row_start += 10000
            
            # if the file does not exist, download it in chuncks
            else: 
                reference_df = download_chunks(validation_table, get_row_count(validation_table, year), filepath=ref_filepath)
                       
            # for all columns in the reference dataframe, remove subpart-specific prefixes
            for col in reference_df:
                reference_df.rename(columns={col : col[len(validation_table) + 1:]}, inplace=True)
                
            # drop any unnamed columns
            if 'unnamed' in reference_df.columns[len(reference_df.columns) - 1].lower() or reference_df.columns[
                len(reference_df.columns) - 1] == '':
                reference_df.drop(reference_df.columns[len(reference_df.columns) - 1], axis=1, inplace=True)
                        
            # parse reference dataframe to prepare it for validation
            reference_df['YEAR'] = reference_df['YEAR'].astype('str')
            reference_df = reference_df[reference_df['YEAR'] == year]
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
            reference_df.reset_index(drop=True, inplace=True)
            
            # save reference dataframe to network
            reference_df.to_csv(ghgrp_external_dir + year + '_GHGRP_NationalTotals.csv', index=False)