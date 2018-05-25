#!/usr/bin/env python
# GHGRP import and processing
# Models with tables available at https://www.epa.gov/enviro/greenhouse-gas-model
# Envirofacts web services documentation can be found at: https://www.epa.gov/enviro/web-services

import stewi.globals as globals
from stewi.globals import import_table
from stewi.globals import drop_excel_sheets
from stewi.globals import download_table
from stewi.globals import set_dir
import pandas as pd
import numpy as np
import requests
from xml.dom import minidom
import os
from datetime import datetime


# Set reporting year to be used in API requests
data_source = 'GHGRP'
report_year = '2013'
output_dir = globals.output_dir
data_dir = globals.data_dir
ghgrp_data_dir = set_dir(data_dir + data_source + '/')

ghgrp_columns = import_table(ghgrp_data_dir + 'ghgrp_columns.csv')
# Column groupings handled based on table structure, which varies by subpart
name_cols = list(ghgrp_columns[ghgrp_columns['ghg_name'] == 1]['column_name'])
quantity_cols = list(ghgrp_columns[ghgrp_columns['ghg_quantity'] == 1]['column_name'])
ch4_cols = list(ghgrp_columns[ghgrp_columns['ch4'] == 1]['column_name'])
n2o_cols = list(ghgrp_columns[ghgrp_columns['n2o'] == 1]['column_name'])
co2_cols = list(ghgrp_columns[ghgrp_columns['co2'] == 1]['column_name'])
co2e_cols = list(ghgrp_columns[ghgrp_columns['co2e_quantity'] == 1]['column_name'])
method_cols = list(ghgrp_columns[ghgrp_columns['method'] == 1]['column_name'])
base_cols = list(ghgrp_columns[ghgrp_columns['base_columns'] == 1]['column_name'])
info_cols = name_cols + quantity_cols + method_cols
group_cols = ch4_cols + n2o_cols + co2_cols# + co2e_cols
ghg_cols = base_cols + info_cols + group_cols


def generate_url(table, report_year='', row_start=0, row_end=9999, output_ext='JSON'):
    # Input a specific table name to generate the query URL to submit
    request_url = enviro_url + table
    if report_year != '': request_url += '/REPORTING_YEAR/=/' + report_year
    if row_start != '': request_url += '/ROWS/' + str(row_start) + ':' + str(row_end)
    request_url += '/' + output_ext
    return request_url


def get_row_count(table, report_year=report_year):
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
                table_temp = import_table(table_url)
                break
            except ValueError: continue
            except: break
        output_table = pd.concat([output_table, table_temp])
        row_start += 10000
    output_table.drop_duplicates(inplace=True)
    if filepath: output_table.to_csv(filepath, index=False)
    return output_table


def get_columns(subpart_name):
    # Input subpart_name from table of subparts, return column names to use in Excel file
    excel_base_cols = ['GHGRP ID', 'Year']
    if subpart_name == 'E':
        excel_quant_cols = ['Total Reported Emissions Under Subpart  E\n(metric tons CO2e)',
                            'Rounded N2O Emissions from Adipic Acid Production']
        excel_method_cols = ['Type of abatement technologies',
                             'Are N2O emissions estimated for this production unit using an Adminstrator-Approved Alternate Method or the Site Specific Emission Factor',
                             'Name of Alternate Method (98.56(k)(1)):',
                             'Description of the Alternate Method (98.56(k)(2)):',
                             'Method Used for the Performance Test']
    elif subpart_name == 'O':
        excel_quant_cols = ['Total Reported Emissions Under Subpart  O\n(metric tons CO2e)',
                            'Rounded HFC-23 emissions (metric tons, output of equation O-4)',
                            'Rounded HFC-23 emissions (metric tons, output of equation O-5)',
                            'Annual mass of HFC-23 emitted from equipment leaks in metric tons',
                            'Annual mass of HFC-23 emitted from all process vents at the facility (metric tons)',
                            'Rounded HFC-23 emissions (from the destruction process/device)']
        excel_method_cols = [
            'Method for tracking startups, shutdowns, and malfuctions and HFC-23 generation/emissions during these events',
            'If any change was made that affects the HFC-23 destruction efficiency or if any change was made to the method used to record the volume destroyed, methods used to determine destruction efficiency.',
            'If any change was made that affects the HFC-23 destruction efficiency or if any change was made to the method used to record the volume destroyed, methods used to record the mass of HFC-23 destroyed.']
    elif subpart_name == 'S':
        excel_quant_cols = ['Total Reported Emissions Under Subpart  S\n(metric tons CO2e)']
        excel_method_cols = ['Method Used to Determine the Quantity of Lime Product Produced and Sold ',
                             'Method Used to Determine the Quantity of Calcined Lime ByProduct/Waste Sold ']
    elif subpart_name == 'BB':
        excel_quant_cols = ['Total Reported Emissions Under Subpart  BB\n(metric tons CO2e)']
        excel_method_cols = [
            'Indicate whether carbon content of the petroleum coke is based on reports from the supplier or through self measurement using applicable ASTM standard method']
    elif subpart_name == 'CC':
        excel_quant_cols = ['Total Reported Emissions Under Subpart  CC\n(metric tons CO2e)',
                            'Annual process CO2 emissions from each manufacturing line']
        excel_method_cols = [
            'Indicate whether CO2 emissions were calculated using a trona input method, a soda ash output method, a site-specific emission factor method, or CEMS']
    elif subpart_name == 'LL':
        excel_quant_cols = ['Total Reported Emissions Under Subpart  LL\n(metric tons CO2e)',
                            'Annual CO2 emissions that would result from the complete combustion or oxidation of all products ']
        excel_method_cols = []
    elif subpart_name == 'RR':
        excel_quant_cols = ['Annual Mass of Carbon Dioxide Sequestered (metric tons)',
                            'Total Mass of Carbon Dioxide Sequestered (metric tons)',
                            'Equation RR-6 Injection Flow Meter Summation (Metric Tons)',
                            'Equation RR-10 Surface Leakage Summation (Metric Tons)',
                            'Mass of CO2 emitted from equipment leaks and vented emissions of CO2 from equipment located on the surface between theflow meter used to measure injection quantity and the injection wellhead (metric tons)',
                            'Mass of CO2 emitted annually from equipment leaks and vented emissions of CO2 from equipment located on the surface between the production wellhead and the flow meter used to measure production quantity (metric tons)',
                            'The entrained CO2 in produced oil or other fluid divided by the CO2 separated through all separators in the reporting year',
                            'Injection Flow Meter:  Mass of CO2 Injected (Metric tons)',
                            'Leakage Pathway:  Mass of CO2 Emitted  (Metric tons)']
        excel_method_cols = ['Equation used to calculate the Mass of Carbond Dioxide Sequestered',
                             'Source(s) of CO2 received', 'CO2 Received: Unit Name', 'CO2 Received:Description',
                             'CO2 Received Unit:  Flow Meter or Container',
                             'CO2 Received Unit:  Mass or Volumetric Basis', 'Injection Flow Meter:  Name or ID',
                             'Injection Flow Meter:  Description', 'Injection Flow meter:  Mass or Volumetric Basis',
                             'Injection Flow meter:  Location', 'Separator Flow Meter:  Description',
                             'Separator Flow meter:  Mass or Volumetric Basis', 'Leakage Pathway:  Name or ID', ]
    return excel_base_cols, excel_quant_cols, excel_method_cols


def get_most_recent_year(report_year=report_year):
    current_year = datetime.now().year
    if int(report_year) >= current_year: raise ValueError('Data not available for ' + report_year + ' yet. Choose an earlier year')
    for i in range(current_year, int(report_year)-1, -1):
        test_url = 'https://www.epa.gov/sites/production/files/' + str(i) + '-12/' + str(i-1) + '_ghgrp_data_summary_spreadsheets.zip'
        if globals.url_is_alive(test_url):
            most_recent_year = str(i - 1)
            break
        elif i == report_year: most_recent_year = '2016'
    return most_recent_year


most_recent_year = get_most_recent_year()
facilities_file = ghgrp_data_dir + most_recent_year + '_ghgrp_data_summary_spreadsheets/' + most_recent_year + ' Data Summary Spreadsheets/ghgp_data_' + report_year + '_8_5_' + str(int(most_recent_year[-2:])+1) + '.xlsx'
facilities_url = 'https://www.epa.gov/sites/production/files/' + str(int(most_recent_year)+1) + '-12/' + most_recent_year + '_ghgrp_data_summary_spreadsheets.zip'
enviro_url = 'https://iaspub.epa.gov/enviro/efservice/'
subparts_url = enviro_url + 'PUB_DIM_SUBPART/JSON'
subparts_file = ghgrp_data_dir + 'subparts.csv'
ghgs_url = enviro_url + 'PUB_DIM_GHG/JSON'
ghgs_file = ghgrp_data_dir + 'ghgs.csv'
# Download link comes from 'https://www.epa.gov/ghgreporting/ghg-reporting-program-data-sets' -- May need to update before running
excel_subparts_url = 'https://www.epa.gov/sites/production/files/' + str(int(most_recent_year)+1) + '-09/e_o_s_cems_bb_cc_ll_rr_full_data_set_8_5_' + str(int(most_recent_year[-2:])+1) + '_final_0.xlsx'
excel_subparts_file = ghgrp_data_dir + 'e_o_s_cems_bb_cc_ll_rr_full_data_set_8_5_' + str(int(most_recent_year[-2:])+1) + '_final_0.xlsx'

required_tables = [[facilities_file, facilities_url], [subparts_file, subparts_url], [ghgs_file, ghgs_url], [excel_subparts_file, excel_subparts_url]]
for table in required_tables: download_table(table[0], url=table[1])

facilities_df = pd.DataFrame()
facilities_dict = import_table(facilities_file, skip_lines=3)
facilities_dict = drop_excel_sheets(facilities_dict, drop_sheets=['Industry Type', 'FAQs about this Data'])
for s in facilities_dict.keys():
    try: facilities_dict[s] = facilities_dict[s][['Facility Id', 'State', 'Primary NAICS Code']]
    except:
        facilities_dict[s] = facilities_dict[s][['Facility Id', 'Reported State', 'Primary NAICS Code']]
        facilities_dict[s].rename(columns={'Reported State': 'State'}, inplace=True)
    facilities_df = pd.concat([facilities_df, facilities_dict[s]]).reset_index(drop=True)
facilities_df = facilities_df.rename(columns={'Facility Id': 'FACILITY_ID'})
facilities_df = facilities_df.rename(columns={'State': 'STATE'})
facilities_df = facilities_df.rename(columns={'Primary NAICS Code': 'NAICS_CODE'})

excel_subparts_dict = import_table(excel_subparts_file)
excel_subparts_dict = drop_excel_sheets(excel_subparts_dict, drop_sheets=['READ ME', None])
subparts = import_table(subparts_file)
ghgs = import_table(ghgs_file)
# Clean up misencoded subscripts
for table in [subparts, ghgs]:
    for column in table.select_dtypes([np.object]):
        table[column] = table[column].str.replace('%3Csub%3E', '').str.replace('%3C/sub%3E', '')


ghgrp0 = pd.DataFrame(columns=ghg_cols)
ghgrp_tables_df = import_table(ghgrp_data_dir + 'all_ghgrp_tables_years.csv').fillna('')
year_tables = ghgrp_tables_df[ghgrp_tables_df['REPORTING_YEAR'].str.contains(report_year)]
year_tables = year_tables[year_tables['PrimaryEmissions'] == 1].reset_index(drop=True)
for index, row in year_tables.iterrows():
    subpart_emissions_table = row['TABLE']
    print(subpart_emissions_table)
    filepath = set_dir(ghgrp_data_dir + 'tables/' + report_year + '/') + subpart_emissions_table + '.csv'
    if os.path.exists(filepath): temp_df = import_table(filepath)
    else:
        subpart_count = get_row_count(subpart_emissions_table, report_year=report_year)
        print('Downloading ' + subpart_emissions_table + '(rows: ' + str(subpart_count) + ')')
        while True:
            try:
                temp_df = download_chunks(table=subpart_emissions_table, table_count=subpart_count, report_year=report_year, filepath=filepath)
                print('Done downloading.')
                break
            except ValueError: continue
            except: break
    for col in temp_df:
        exec("temp_df = temp_df.rename(columns={'" + col + "':'" + col[len(subpart_emissions_table) + 1:] + "'})")
    if 'unnamed' in temp_df.columns[len(temp_df.columns) - 1].lower() or temp_df.columns[len(temp_df.columns) - 1] == '':
        temp_df.drop(temp_df.columns[len(temp_df.columns) - 1], axis=1, inplace=True)
    temp_df['SUBPART_NAME'] = row['SUBPART']
    ghgrp0 = pd.concat([ghgrp0, temp_df])

# Combine equivalent columns from different tables into one, delete old columns
ghgrp1 = ghgrp0[ghg_cols]
ghgrp1['Amount'] = ghgrp1[quantity_cols].fillna(0).sum(axis=1)
ghgrp1['Flow Description'] = ghgrp1[name_cols].fillna('').sum(axis=1)
ghgrp1['METHOD'] = ghgrp1[method_cols].fillna('').sum(axis=1)

ghgrp1.drop(info_cols, axis=1, inplace=True)
ghgrp1.drop(group_cols, axis=1, inplace=True)
ghgrp1 = ghgrp1[ghgrp1['Flow Description'] != '']

ghgrp2 = pd.DataFrame()
group_list = [ch4_cols, n2o_cols, co2_cols]#, co2e_cols]
for group in group_list:
    for i in range(0, len(group)):
        ghg_cols2 = base_cols + [group[i]] + method_cols
        temp_df = ghgrp0[ghg_cols2]
        temp_df = temp_df[pd.notnull(temp_df[group[i]])]
        temp_df['Flow Description'] = group[i]
        temp_df['METHOD'] = temp_df[method_cols].fillna('').sum(axis=1)
        temp_df.drop(method_cols, axis=1, inplace=True)
        temp_df.rename(columns={group[i]: 'Amount'}, inplace=True)
        ghgrp2 = pd.concat([ghgrp2, temp_df])

ghgrp3 = pd.DataFrame()
excel_dict = {'Adipic Acid': 'E', 'HCFC-22 Prod. HFC-23 Dest.': 'O', 'Lime': 'S', 'Silicon Carbide': 'BB',
              'Soda Ash': 'CC', 'CoalBased Liquid Fuel Suppliers': 'LL', 'Geologic Sequestration of CO2': 'RR'}
excel_keys = list(excel_subparts_dict.keys())

for key in excel_keys:
    excel_base_cols, excel_quant_cols, excel_method_cols = get_columns(excel_dict[key])
    temp_cols = excel_base_cols + excel_quant_cols + excel_method_cols
    temp_df = excel_subparts_dict[key][temp_cols]  # .to_frame()
    temp_df['METHOD'] = temp_df[excel_method_cols].fillna('').sum(axis=1)
    for col in excel_quant_cols:
        col_df = temp_df.dropna(subset=[col])
        col_df = col_df[col_df['Year'] == int(report_year)]
        col_df['SUBPART_NAME'] = excel_dict[key]
        col_df['Flow Description'] = col
        col_df['Amount'] = col_df[col]
        col_df.drop(excel_method_cols, axis=1, inplace=True)
        col_df.drop(excel_quant_cols, axis=1, inplace=True)
        ghgrp3 = pd.concat([ghgrp3, col_df])
ghgrp3 = ghgrp3.rename(columns={'GHGRP ID': 'FACILITY_ID'})
ghgrp3.drop('Year', axis=1, inplace=True)
reliability_table = globals.reliability_table
ghgrp_reliability_table = reliability_table[reliability_table['Source'] == 'GHGRPa']
ghgrp_reliability_table.drop('Source', axis=1, inplace=True)

# Map flow descriptions to standard gas names from GHGRP
ghg_mapping = pd.read_csv(ghgrp_data_dir + 'ghg_mapping.csv', usecols=['Flow Description', 'FlowName'])

# Merge tables
ghgrp = pd.concat([ghgrp1, ghgrp2, ghgrp3]).reset_index(drop=True)
ghgrp = ghgrp.merge(facilities_df, on='FACILITY_ID', how='left')
ghgrp = pd.merge(ghgrp, ghgrp_reliability_table, left_on='METHOD', right_on='Code', how='left')
ghgrp = pd.merge(ghgrp, ghg_mapping, on='Flow Description', how='left')

# Fill NAs with 5 for DQI reliability score
ghgrp.replace('', np.nan)
ghgrp['DQI Reliability Score'] = ghgrp['DQI Reliability Score'].fillna(value=5)
ghgrp.drop('Code', axis=1, inplace=True)
ghgrp['Amount'] = 1000 * ghgrp['Amount']
ghgrp.drop('METHOD', axis=1, inplace=True)
ghgrp.rename(columns={'FACILITY_ID': 'FacilityID'}, inplace=True)
ghgrp.rename(columns={'DQI Reliability Score': 'ReliabilityScore'}, inplace=True)
ghgrp.rename(columns={'NAICS_CODE': 'NAICS'}, inplace=True)

# TODO: Validation
# Compare with national totals if available.
# Also, check that all expected facilities are accounted for in each subpart based on 'PUB_DIM_FACILITY'
# Compare amounts at facility level

# validation_table = 'PUB_FACTS_SUBP_GHG_EMISSION'
# validation_df = download_chunks(validation_table, get_row_count(validation_table), report_year=report_year)
# validation_df = validation_df.merge()


output_file = output_dir + data_source + '_' + report_year + '.csv'
ghgrp.to_csv(output_file, index=False)

