#!/usr/bin/env python

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
    flowbyfacility_fields, flowbySCC_fields, facility_fields
import pandas as pd
import numpy as np
import requests
from xml.dom import minidom
import os
from datetime import datetime
import argparse
import logging as log


## define directories
data_dir = data_dir # stewi data directory
output_dir = output_dir # stewi output directory
ghgrp_data_dir = set_dir(data_dir + 'ghgrp/') # stewi data directory --> ghgrp
ghgrp_external_dir = set_dir(data_dir + '/../../../GHGRP Data Files/') # external GHGRP data directory
   

def generate_url(table, report_year='', row_start=0, row_end=9999, output_ext='JSON'):
    # Input a specific table name to generate the query URL to submit
    request_url = enviro_url + table
    if report_year != '': request_url += '/REPORTING_YEAR/=/' + report_year
    if row_start != '': request_url += '/ROWS/' + str(row_start) + ':' + str(row_end)
    request_url += '/' + output_ext
    return request_url


def get_row_count(table, report_year):
    # Input specific table name, returns number of rows from API as XML then converts to integer
    count_url = enviro_url + table
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
    # do not import the Subpart L table; we will pull these data in later
    year_tables.drop(year_tables[year_tables['TABLE'] == 'EF_L_GASINFO' ].index, inplace=True)
    
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
    ghgrp1b.drop(ghgrp1b.columns.difference(base_cols + expanded_group_cols + ['METHOD', 'SUBPART_NAME']),1, inplace=True)
    # 'unpivot' data to create separate line items for each group column
    ghgrp1b = ghgrp1b.melt(id_vars = base_cols + ['METHOD', 'SUBPART_NAME'], 
                           var_name = 'Flow Description', 
                           value_name = 'FlowAmount')

    # re-join split dataframes
    ghgrp1 = pd.concat([ghgrp1a, ghgrp1b]).reset_index(drop=True)
    
    # drop those rows where flow amount is confidential
    ghgrp1 = ghgrp1[ghgrp1['FlowAmount'] != 'confidential']

    return ghgrp1


def parse_additional_suparts_data(addtnl_subparts_path, addtnl_subparts_columns, year):
    
    # load .xslx data for additional subparts from filepath
    addtnl_subparts_dict = import_table(addtnl_subparts_path)
    for key, df in addtnl_subparts_dict.items():
        for column in df:
            df.rename(columns={column: column.replace('\n',' ')}, inplace=True)
            df.rename(columns={'Facility ID' : 'GHGRP ID'}, inplace=True)
            df.rename(columns={'Reporting Year' : 'Year'}, inplace=True)
        addtnl_subparts_dict[key] = df
    # initialize dataframe
    ghgrp = pd.DataFrame()

    # import column headers data for additional subparts
    addtnl_subparts_cols = import_table(ghgrp_data_dir + addtnl_subparts_columns)
    
    # get list of tabs to process
    addtnl_tabs = addtnl_subparts_cols['tab_name'].unique()
    
    addtnl_base_cols = ['GHGRP ID', 'Year']
    
    # for each of the tabs in the excel workbook...
    for tab in addtnl_tabs:
        
        # get quantity columns
        addtnl_quant_cols = list(addtnl_subparts_cols.loc[
            np.where((addtnl_subparts_cols['tab_name'] == tab) & 
                     (addtnl_subparts_cols['column_type'] == 'quantity'))]['column_name'])
        
        # get method columns
        addtnl_method_cols = list(addtnl_subparts_cols.loc[
            np.where((addtnl_subparts_cols['tab_name'] == tab) & 
                     (addtnl_subparts_cols['column_type'] == 'method'))]['column_name'])

        # create temporary dataframe from worksheet, using just the desired columns      
        temp_df = addtnl_subparts_dict[tab][addtnl_base_cols + addtnl_quant_cols + addtnl_method_cols]
        
        # combine all method equation columns into one, drop old method columns
        temp_df['METHOD'] = temp_df[addtnl_method_cols].fillna('').sum(axis=1)
        temp_df.drop(addtnl_method_cols, axis=1, inplace=True)
        
        # keep only those data for the specified report year
        temp_df = temp_df[temp_df['Year'] == int(year)]
        
        # 'unpivot' data to create separate line items for each quantity column
        temp_df = temp_df.melt(id_vars = addtnl_base_cols + ['METHOD'], 
                               var_name = 'Flow Description', 
                               value_name = 'FlowAmount')
        
        # drop those rows where flow amount is confidential
        temp_df = temp_df[temp_df['FlowAmount'] != 'confidential']
        
        # add 1-2 letter subpart abbreviation
        temp_df['SUBPART_NAME'] = (list(addtnl_subparts_cols.loc[addtnl_subparts_cols['tab_name'] == tab, 'subpart_abbr'])[0])
        
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
    
    # most recent year of available GHGRP data
    most_recent_year = '2018'
    
    ## define urls for data to be pulled from the web
    
    # Data Summary Spreadsheets: .zip file containing multi-year spreadsheets containing
    # the most important high-level information for facilities, as well as yearly
    # spreadsheets containing slightly more detailed information, including
    # reported emissions by greenhouse gas and process.
    data_summaries_url = 'https://www.epa.gov/sites/production/files/2019-10/2018_data_summary_spreadsheets.zip'
    
    # Subparts E, S-CEMS, BB, CC, LL Data Set: .xlsx file containing publicly available data
    # reported by facilities in the following industries across all reporting years:
    # Adipic Acid Production (E), Lime Manufacturing (S, CEMS reporters only), 
    # Silicon Carbide Production (BB), Soda Ash Manufacturing (CC) and 
    # Coal-based Liquid Fuel Suppliers (LL).
    # LL is not included because it is a supplier, rather than an emitter.
    esbb_subparts_url = 'https://www.epa.gov/sites/production/files/2019-10/e_s_cems_bb_cc_ll_full_data_set_9_29_2019.xlsx'
    
    # Subparts L, O Data Set: .xlsx file containing publicly available data
    # reported by facilities under: 
    # Subpart L (Fluorinated Gas Production) and 
    # Subpart O (HCFC-22 Production and HFC-23 Destruction)
    # across all reporting years.
    lo_subparts_url = 'https://www.epa.gov/sites/production/files/2020-11/ghgp_l_o_freq_request_data_10_2020.xlsx'
    
    # Envirofacts: root url for EPA's Envirofacts Data Service
    enviro_url = 'https://data.epa.gov/efservice/'
    
    # define GWPs
    # (these values are from IPCC's AR4, which is consistent with GHGRP methodology)
    CH4GWP = 25
    N2OGWP = 298
    HCFC22GWP = 14800
    
    # define column groupings
    ghgrp_columns = import_table(ghgrp_data_dir + 'ghgrp_columns.csv')
    name_cols = list(ghgrp_columns[ghgrp_columns['ghg_name'] == 1]['column_name'])
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
    data_summaries_path = ghgrp_external_dir + most_recent_year + '_data_summary_spreadsheets/'
    esbb_subparts_path = ghgrp_external_dir + esbb_subparts_url[51:]
    lo_subparts_path = ghgrp_external_dir + lo_subparts_url[51:]
    
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
            required_tables = [[data_summaries_path, data_summaries_url, 'Static File'], 
                               [esbb_subparts_path, esbb_subparts_url, 'Static File'],
                               [lo_subparts_path, lo_subparts_url, 'Static File'],
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

            # convert subpart O data from CO2e to mass of HCFC22 emitted
            ghgrp3.loc[ghgrp3['SUBPART_NAME'] == 'O', 'FlowAmount'] = \
                ghgrp3.loc[ghgrp3['SUBPART_NAME'] == 'O', 'FlowAmount']/HCFC22GWP
            ghgrp3.loc[ghgrp3['SUBPART_NAME'] == 'O', 'Flow Description'] = \
                'Total Reported Emissions Under Subpart  O (metric tons)'
                
            # parse emissions data for subpart L
            ghgrp4 = parse_additional_suparts_data(lo_subparts_path, 'l_subparts_columns.csv', year)

            # load global warming potentials for subpart L calculation
            subpart_L_GWPs_url = 'https://ccdsupport.com/confluence/download/attachments/'\
                '63996073/Subpart%20L%20Calculation%20Spreadsheet%20-%20Summarize%20Process'\
                '%20Level%20CO2e%20by%20f-GHG.xls?version=1&modificationDate=1427459649000&api=v2'
            subpart_L_GWPs_filepath = ghgrp_external_dir + 'Subpart L Calculation Spreadsheet.xls' 
            download_table(filepath = subpart_L_GWPs_filepath, url = subpart_L_GWPs_url)
            subpart_L_GWPs = pd.read_excel(subpart_L_GWPs_filepath, sheet_name = 'Lookup Tables', usecols = [6,7], nrows=12)
            
            # rename certain fluorinated GHG groups for consistency
            subpart_L_GWPs['Fluorinated GHG Groupd'] = subpart_L_GWPs['Fluorinated GHG Groupd'].str.replace(
                'Saturated HFCs with 2 or fewer carbon-hydrogen bonds',
                'Saturated hydrofluorocarbons (HFCs) with 2 or fewer carbon-hydrogen bonds')
            subpart_L_GWPs['Fluorinated GHG Groupd'] = subpart_L_GWPs['Fluorinated GHG Groupd'].str.replace(
                'Saturated HFEs and HCFEs with 1 carbon-hydrogen bond',
                'Saturated hydrofluoroethers (HFEs) and hydrochlorofluoroethers (HCFEs) with 1 carbon-hydrogen bond')
            subpart_L_GWPs['Fluorinated GHG Groupd'] = subpart_L_GWPs['Fluorinated GHG Groupd'].str.replace(
                'Unsaturated PFCs, unsaturated HFCs, unsaturated HCFCs, unsaturated halogenated ethers, unsaturated halogenated esters, fluorinated aldehydes, and fluorinated ketones',
                'Unsaturated perfluorocarbons (PFCs), unsaturated HFCs, unsaturated hydrochlorofluorocarbons (HCFCs), unsaturated halogenated ethers, unsaturated halogenated esters, fluorinated aldehydes, and fluorinated ketones')

            # add global warming potentials for each flow
            subpart_L_GWPs['Flow Description'] = 'Fluorinated GHG Emissions (mt CO2e)'
            ghgrp4 = ghgrp4.merge(subpart_L_GWPs, how='left', left_on=['METHOD','Flow Description'], right_on=['Fluorinated GHG Groupd','Flow Description'])
            ghgrp4.loc[ghgrp4['Flow Description'] == 'Fluorinated GHG Emissions (metric tons)', 'Default Global Warming Potential'] = 1
            ghgrp4.loc[ghgrp4['Flow Description'] == 'Fluorinated GHG Emissions  (metric tons)', 'Default Global Warming Potential'] = 1
            
            # drop old Flow Description column
            ghgrp4.drop(columns=['Flow Description'], inplace=True)
            # METHOD column becomes new Flow Description
            ghgrp4.rename(columns={'METHOD' : 'Flow Description'}, inplace=True)
            # calculate mass flow amount based on emissions in CO2e and GWP
            ghgrp4['FlowAmount (mass)'] = ghgrp4['FlowAmount'] / ghgrp4['Default Global Warming Potential']
            # drop unnecessary columns
            ghgrp4.drop(columns=['FlowAmount', 'Default Global Warming Potential', 'Fluorinated GHG Groupd'], inplace=True)
            # rename Flow Amount column
            ghgrp4.rename(columns={'FlowAmount (mass)' : 'FlowAmount'}, inplace=True)
                
            # concatenate ghgrp1, ghgrp2, ghgrp3, and ghgrp4
            ghgrp = pd.concat([ghgrp1, ghgrp2, ghgrp3, ghgrp4]).reset_index(drop=True)
                        
            # map flow descriptions to standard gas names from GHGRP
            ghg_mapping = pd.read_csv(ghgrp_data_dir + 'ghg_mapping.csv', usecols=['Flow Description', 'FlowName', 'FlowID'])
            ghgrp = pd.merge(ghgrp, ghg_mapping, on='Flow Description', how='left')
            ghgrp.drop('Flow Description', axis=1, inplace=True)
            
            # rename certain columns for consistency
            ghgrp.rename(columns={'FACILITY_ID':'FacilityID',
                                  'NAICS_CODE':'NAICS'}, inplace=True)    
            
            # pickle data and save to network
            log.info('saving GHGRP data to pickle')
            ghgrp.to_pickle('work/GHGRP_' + year + '.pk')
                                           
        # if any option other than 'A' is selected, load the ghgrp dataframe from the local network
        else:
            log.info('extracting data from GHGRP pickle')
            ghgrp = pd.read_pickle('work/GHGRP_' + year + '.pk')
                               
        if args.Option == 'B':
            
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
            flow_columns = ['FlowName', 'FlowID']
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
            
            log.info('validating flowbyfacility against national totals')
                   
            # import list of GHG names
            ghgs_df = import_table(ghgrp_external_dir + 'ghgs.csv')
            ghgs = list(ghgs_df['GAS_NAME'])
            # fix subscripts
            ghgs = [sub.replace('%3Csub%3E', '') for sub in ghgs]
            ghgs = [sub.replace('%3C/sub%3E', '') for sub in ghgs]
            # delete ; we won't be using this
            ghgs.remove('Fluorinated GHG Production (CO2e)')
            
            # import list of subpart names
            subparts_df = import_table(ghgrp_external_dir + 'subparts.csv')
            subparts = list(subparts_df['SUBPART_NAME'])
            subparts.remove('C (Abbr)')
            
            # initialize dataframe
            df0 = pd.DataFrame(index=subparts, columns=ghgs)
            
            # create pivot table from reference data
            reference_df = import_table(ghgrp_external_dir + year + '_GHGRP_NationalTotals.csv')
            ref_df = pd.pivot_table(reference_df, values='FlowAmount', index='SUBPART_NAME', columns='FlowName')
            ref_df = ref_df.reindex_like(df0)
            
            # create pivot table from tabulated data
            tab_df = pd.pivot_table(ghgrp_fbs, values='FlowAmount', index='SCC', columns='FlowName')
            tab_df = tab_df.reindex_like(df0)
            
            # calculate percentage difference
            pct_df = 100.0 * (tab_df - ref_df) / ref_df
            
            # generate conclusion
            cncl_df = df0
            tolerance=5.0
            for row in (df0.index):
                for col in (df0):
                    tab_value = tab_df.loc[row, col]
                    ref_value = ref_df.loc[row, col]
                    if tab_value == 0.0 and ref_value == 0.0:
                        cncl_df.loc[row, col] = 'Both inventory and reference are zero'
                    elif pd.isnull(tab_value) and pd.isnull(ref_value):
                        cncl_df.loc[row, col] = 'Both inventory and reference are null'
                    elif pd.isnull(tab_value):
                        cncl_df.loc[row, col] = 'Inventory value is null'
                    elif pd.isnull(ref_value):
                        cncl_df.loc[row, col] = 'Reference value is null'
                    else:
                        pct_diff = pct_df.loc[row, col]
                        if pct_diff == 0.0:
                            cncl_df.loc[row, col] = 'Identical'
                        elif abs(pct_diff) <= tolerance:
                            cncl_df.loc[row, col] = 'Statistically similar'
                        elif abs(pct_diff) > tolerance:
                            cncl_df.loc[row, col] = 'Percent difference exceeds tolerance'
            
            # write results to Excel workbook            
            writer = pd.ExcelWriter(output_dir + 'validation/' + 'GHGRP_' + year + '.xlsx', engine='xlsxwriter')
            ref_df.to_excel(writer, sheet_name='reference')
            tab_df.to_excel(writer, sheet_name='tabulated')
            pct_df.to_excel(writer, sheet_name='percent_difference')
            cncl_df.to_excel(writer, sheet_name='conclusion')
            writer.save()

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
            reference_df = reference_df[['FlowAmount', 'GAS_NAME', 'FACILITY_ID', 'SUBPART_NAME']]
            reference_df.rename(columns={'FACILITY_ID': 'FacilityID',
                                         'GAS_NAME': 'FlowName'}, inplace=True)
            reference_df.reset_index(drop=True, inplace=True)
            
            # save reference dataframe to network
            reference_df.to_csv(ghgrp_external_dir + year + '_GHGRP_NationalTotals.csv', index=False)