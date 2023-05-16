# RCRAInfo.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Download specified Biennial Report files from EPA RCRAInfo system for specified year
This file requires parameters be passed like:
Option -Y Year -T Table1 Table2 ... TableN
where Option is either A, B, C:
Options
A - for extracting files from RCRAInfo site
B - organize files by year
C - for processing Biennial Report into flowbyfacility, validation, and metadata saving
Year is like 2015 with coverage up for 2011, 2013, 2015
# List of tables:
        ### BR_GM_WASTE_CODE
        ### BR_LU_DENSITY_UOM
        ### BR_LU_FORM_CODE
        ### BR_LU_MANAGEMENT_METHOD
        ### BR_LU_SOURCE_CODE
        ### BR_LU_UOM
        ### BR_LU_WASTE_MINIMIZATION
        ### BR_REPORTING_year
        ### BR_WR_WASTE_CODE
        ### CE_CITATION
        ### CE_LU_CITATION
        ### CE_REPORTING
        ### CA_AREA
        ### CA_AREA_EVENT
        ### CA_AREA_UNIT
        ### CA_AUTHORITY
        ### CA_AUTHORITY_CITATION
        ### CA_EVENT
        ### CA_EVENT_AUTHORITY
        ### CA_LU_AUTHORITY
        ### CA_LU_EVENT_CODE
        ### CA_LU_STATUTORY_CITATION
        ### EM_REPORTING
        ### FA_COST_ESTIMATE
        ### FA_COST_MECHANISM_DETAIL
        ### FA_LU_MECHANISM_TYPE
        ### FA_MECHANISM
        ### FA_MECHANISM_DETAIL
        ### GS_GIS
        ### GS_GIS_LAT_LONG
        ### GS_LU_AREA_SOURCE
        ### GS_LU_COORDINATE
        ### GS_LU_GEOGRAPHIC_REFERENCE
        ### GS_LU_GEOMETRIC
        ### GS_LU_HORIZONTAL_COLLECTION
        ### GS_LU_HORIZONTAL_REFERENCE
        ### GS_LU_VERIFICATION
        ### HD_BASIC
        ### HD_CERTIFICATION
        ### HD_EPISODIC_EVENT
        ### HD_EPISODIC_WASTE
        ### HD_EPISODIC_WASTE_CODE
        ### HD_HANDLER
        ### HD_HSM_ACTIVITY
        ### HD_HSM_BASIC
        ### HD_HSM_RECYCLER
        ### HD_HSM_WASTE_CODE
        ### HD_LQG_CLOSURE
        ### HD_LQG_CONSOLIDATION
        ### HD_LU_COUNTRY
        ### HD_LU_COUNTY
        ### HD_LU_EPISODIC_EVENT
        ### HD_LU_FOREIGN_STATE
        ### HD_LU_GENERATOR_STATUS
        ### HD_LU_HSM_FACILITY_CODE
        ### HD_LU_NAICS
        ### HD_LU_OTHER_PERMIT
        ### HD_LU_RELATIONSHIP
        ### HD_LU_STATE
        ### HD_LU_STATE_ACTIVITY
        ### HD_LU_STATE_DISTRICT
        ### HD_LU_UNIVERSAL_WASTE
        ### HD_LU_WASTE_CODE
        ### HD_NAICS
        ### HD_OTHER_ID
        ### HD_OTHER_PERMIT
        ### HD_OWNER_OPERATOR
        ### HD_PART_A
        ### HD_REPORTING
        ### HD_STATE_ACTIVITY
        ### HD_UNIVERSAL_WASTE
        ### HD_WASTE_CODE
        ### PM_EVENT
        ### PM_EVENT_UNIT_DETAIL
        ### PM_LU_LEGAL_OPERATING_STATUS
        ### PM_LU_PERMIT_EVENT_CODE
        ### PM_LU_PROCESS_CODE
        ### PM_LU_UNIT_OF_MEASURE
        ### PM_SERIES
        ### PM_UNIT
        ### PM_UNIT_DETAIL
        ### PM_UNIT_DETAIL_WASTE
See more documentation of files at https://rcrapublic.epa.gov/rcrainfoweb/
"""

import pandas as pd
import zipfile
import argparse
import re
import os
import time
import datetime
from pathlib import Path

from esupy.processed_data_mgmt import read_source_metadata
from stewi.globals import write_metadata, DATA_PATH, config,\
    USton_kg, get_reliability_table_for_source, paths,\
    log, store_inventory, compile_source_metadata,\
    aggregate, set_stewi_meta
from stewi.validate import update_validationsets_sources, validate_inventory,\
    write_validation_result
from stewi.filter import apply_filters_to_inventory
import stewi.exceptions

try:
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    log.error('Must install selenium and webdriver_manager for RCRAInfo. '
              'See install instructions for optional package '
              'installation or install them indepedently and retry.')


_config = config()['databases']['RCRAInfo']
EXT_DIR = 'RCRAInfo Data Files'
OUTPUT_PATH = Path(paths.local_path).joinpath(EXT_DIR)
RCRA_DATA_PATH = DATA_PATH / 'RCRAInfo'
DIR_RCRA_BY_YEAR = OUTPUT_PATH.joinpath('RCRAInfo_by_year')


def waste_description_cleaner(x):
    if ('from br conversion' in x) or (x == 'From 1989 BR data'):
        x = None
    return x


def extracting_files(path_unzip, name):
    with zipfile.ZipFile(path_unzip.joinpath(name + '.zip')) as z:
        z.extractall(path_unzip)
    log.info(f'{name} stored to {path_unzip}')
    os.remove(path_unzip.joinpath(name + '.zip'))


def download_and_extract_zip(tables, query):
    log.info('Initiating download via browswer...')
    regex = re.compile(r'(.+).zip\s?\(\d+.?\d*\s?[a-zA-Z]{2,}\)')
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-notifications')
    options.add_argument('--no-sandbox')
    options.add_argument('--verbose')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--log-level=3')
    options.add_argument('--hide-scrollbars')
    prefs = {'download.default_directory': str(OUTPUT_PATH),
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing_for_trusted_sources_enabled': False,
            'safebrowsing.enabled': False}
    options.add_experimental_option('prefs', prefs)
    browser = webdriver.Chrome(ChromeDriverManager().install(),
                               options=options)
    browser.maximize_window()
    browser.set_page_load_timeout(30)
    browser.get(_config['url'])
    time.sleep(5)
    table_of_tables = browser.find_element_by_xpath(query)
    rows = table_of_tables.find_elements_by_css_selector('tr')[1:] # Excluding header
    # Extracting zip files for Biennial Report Tables
    Links = {}
    for row in rows:
        loop = 'YES'
        while loop == 'YES':
            try:
                table_name = re.search(
                    regex, row.find_elements_by_css_selector('td')[3].text
                    ).group(1)
                Link = row.find_elements_by_css_selector('td')[3]\
                    .find_elements_by_css_selector('a')[0]\
                        .get_attribute('href')
                Links.update({table_name: Link})
                loop = 'NO'
            except AttributeError:
                loop = 'YES'
                now = datetime.datetime.now()
                print('AttributeError occurred with selenium due to not '
                      'appropriate charging of website.\nHour: '
                      '{}:{}:{}'.format(now.hour, now.minute, now.second))
    # Download the desired zip
    if tables == [None]:
        tables = list(Links.keys())
    log.info(f'If download fails, locate {tables} and save zip file to {OUTPUT_PATH}'
             ' and code will proceed')
    for name in tables:
        if OUTPUT_PATH.joinpath(f"{name}_0.csv").is_file():
            log.info(f"{name} found in {OUTPUT_PATH}, skipping")
            continue
        browser.get(Links[name])
        condition = OUTPUT_PATH.joinpath(f"{name}.zip").is_file()
        while condition is False:
            # continue to check for downloaded zip file
            condition = OUTPUT_PATH.joinpath(f"{name}.zip").is_file()
        time.sleep(5)
        extracting_files(OUTPUT_PATH, name)
    log.info('file extraction complete')
    browser.quit()


def organize_br_reporting_files_by_year(tables, year):
    """Consolidate BR_REPORTING files to single csv."""
    year = int(year)
    for table in tables:
        if 'BR_REPORTING' in table:
            log.info(f'organizing data for {table} from {str(year)}...')
            linewidthsdf = pd.read_csv(RCRA_DATA_PATH
                                       .joinpath('RCRA_FlatFile_LineComponents.csv'))
            fields = linewidthsdf['Data Element Name'].tolist()
            files = sorted([file for file in OUTPUT_PATH
                            .glob(f'{table}*{str(year)}*.csv')])
            df_full = pd.DataFrame()
            for filepath in files:
                log.info(f'extracting {filepath}')
                df = pd.read_csv(filepath, header=0,
                                 usecols=list(range(0, len(fields))),
                                 names=fields,
                                 low_memory=False,
                                 encoding='utf-8')
                df = df[df['Report Cycle'].apply(
                    lambda x: str(x).replace('.0', '').isdigit())]
                if df['Location Street Number'].dtype != 'str':
                    df['Location Street Number'] = df['Location Street Number'].astype(str)
                    df['Location Street Number'] = df['Location Street Number'].apply(
                        lambda x: str(x).replace('.0', ''))
                df['Report Cycle'] = df['Report Cycle'].astype(int)
                df = df[df['Report Cycle'] == year]
                df_full = pd.concat([df_full, df])
            DIR_RCRA_BY_YEAR.mkdir(exist_ok=True)
            filepath = DIR_RCRA_BY_YEAR.joinpath(f'br_reporting_{str(year)}.csv')
            log.info(f'saving to {filepath}...')
            df_full.to_csv(filepath, index=False)
            generate_metadata(year, files, datatype='source')
        else:
            log.info(f'skipping {table}')


def Generate_RCRAInfo_files_csv(report_year):
    """Generate stewi inventory files from downloaded data files."""
    log.info(f'generating inventory files for {report_year}')
    filepath = DIR_RCRA_BY_YEAR.joinpath(f'br_reporting_{str(report_year)}.csv')
    # Get columns to keep
    fieldstokeep = pd.read_csv(RCRA_DATA_PATH.joinpath('RCRA_required_fields.txt'),
                               header=None)
    # on_bad_lines requires pandas >= 1.3
    df = pd.read_csv(filepath, header=0, usecols=list(fieldstokeep[0]),
                     low_memory=False, on_bad_lines='skip',
                     encoding='ISO-8859-1')

    log.info(f'completed reading {filepath}')
    # Checking the Waste Generation Data Health
    df = df[pd.to_numeric(df['Generation Tons'], errors='coerce').notnull()]
    df['Generation Tons'] = df['Generation Tons'].astype(float)
    log.debug(f'number of records: {len(df)}')
    # Reassign the NAICS to a string
    df['NAICS'] = df['Primary NAICS'].astype('str')
    df.drop(columns=['Primary NAICS'], inplace=True)
    # Create field for DQI Reliability Score with fixed value from CSV
    rcrainfo_reliability_table = get_reliability_table_for_source('RCRAInfo')
    df['DataReliability'] = float(rcrainfo_reliability_table['DQI Reliability Score'])
    # Create a new field to put converted amount in
    df['Amount_kg'] = 0.0
    # Convert amounts from tons. Note this could be replaced with a conversion utility
    df['Amount_kg'] = USton_kg * df['Generation Tons']
    # Read in waste descriptions
    linewidthsdf = pd.read_csv(RCRA_DATA_PATH
                               .joinpath('RCRAInfo_LU_WasteCode_LineComponents.csv'))
    names = linewidthsdf['Data Element Name']
    try:
        wastecodesfile = [file for file in OUTPUT_PATH.glob('*lu_waste_code*.csv')][0]
    except IndexError:
        log.exception('waste codes file missing, download and unzip waste code'
                      f' file to {OUTPUT_PATH}')
    waste_codes = pd.read_csv(wastecodesfile,
                              header=0,
                              names=names)
    # Remove rows where any fields are na description is missing
    waste_codes = waste_codes[['Waste Code', 'Code Type',
                               'Waste Code Description']].dropna()
    waste_codes['Waste Code Description'] = waste_codes[
        'Waste Code Description'].apply(waste_description_cleaner)
    waste_codes = waste_codes.drop_duplicates(ignore_index=True)
    waste_codes = waste_codes[~((waste_codes['Waste Code'].duplicated(False)) &
                                ((waste_codes['Waste Code Description'].isna()) |
                                 (waste_codes['Waste Code Description'] == 'Unknown')))]
    waste_codes.rename(columns={'Waste Code': 'Waste Code Group',
                                'Code Type': 'Waste Code Type'}, inplace=True)
    df = df.merge(waste_codes, on='Waste Code Group', how='left')

    # Replace form code with the code name
    form_code_name_file = RCRA_DATA_PATH.joinpath('RCRA_LU_FORM_CODE.csv')
    form_code_name_df = pd.read_csv(form_code_name_file, header=0,
                                    usecols=['FORM_CODE', 'FORM_CODE_NAME'])
    form_code_name_df.rename(columns={'FORM_CODE': 'Form Code'}, inplace=True)
    df = df.merge(form_code_name_df, on='Form Code', how='left')

    df['FlowName'] = df['Waste Code Description']

    # If there is not useful waste code, fill it with the Form Code Name
    # Find the NAs in FlowName and then give that source of Form Code
    df.loc[df['FlowName'].isnull(), 'FlowNameSource'] = 'Form Code'
    df.loc[df['FlowNameSource'].isnull(), 'FlowNameSource'] = 'Waste Code'
    # Set FlowIDs to the appropriate code
    df.loc[df['FlowName'].isnull(), 'FlowID'] = df['Form Code']
    df.loc[df['FlowID'].isnull(), 'FlowID'] = df['Waste Code Group']
    df['FlowName'].fillna(df['FORM_CODE_NAME'], inplace=True)
    df = df.dropna(subset=['FlowID']).reset_index(drop=True)
    drop_fields = ['Generation Tons',
                   'Management Method', 'Waste Description',
                   'Waste Code Description', 'FORM_CODE_NAME']
    df.drop(columns=drop_fields, inplace=True)
    # Rename cols used by multiple tables
    df.rename(columns={'Handler ID': 'FacilityID',
                       'Amount_kg': 'FlowAmount'}, inplace=True)

    # Prepare flows file
    flows = df[['FlowName', 'FlowID', 'FlowNameSource']]
    flows = flows.drop_duplicates(ignore_index=True)
    # Sort them by the flow names
    flows.sort_values(by='FlowName', axis=0, inplace=True)
    store_inventory(flows, 'RCRAInfo_' + report_year, 'flow')

    # Prepare facilities file
    facilities = df[['FacilityID', 'Handler Name', 'Location Street Number',
                     'Location Street 1', 'Location Street 2', 'Location City',
                     'Location State', 'Location Zip', 'County Name',
                     'NAICS', 'Generator ID Included in NBR']].reset_index(drop=True)
    facilities.drop_duplicates(inplace=True, ignore_index=True)
    facilities['Address'] = facilities[['Location Street Number',
                                        'Location Street 1',
                                        'Location Street 2']].apply(
                                            lambda x: ' '.join(x.dropna()),
                                            axis=1)
    facilities.drop(columns=['Location Street Number', 'Location Street 1',
                             'Location Street 2'], inplace=True)
    facilities.rename(columns={'Primary NAICS': 'NAICS',
                               'Handler Name': 'FacilityName',
                               'Location City': 'City',
                               'Location State': 'State',
                               'Location Zip': 'Zip',
                               'County Name': 'County'}, inplace=True)
    store_inventory(facilities, 'RCRAInfo_' + report_year, 'facility')
    # Prepare flow by facility
    flowbyfacility = aggregate(df, ['FacilityID', 'FlowName', 'Source Code',
                                    'Generator Waste Stream Included in NBR'])
    store_inventory(flowbyfacility, 'RCRAInfo_' + report_year, 'flowbyfacility')

    validate_state_totals(report_year, flowbyfacility)

    # Record metadata
    generate_metadata(report_year, filepath, datatype='inventory')


def generate_metadata(year, files, datatype='inventory'):
    """Get metadata and writes to .json."""
    if datatype == 'source':
        source_path = [str(p) for p in files]
        source_meta = compile_source_metadata(source_path, _config, year)
        source_meta['SourceType'] = 'Zip file'
        source_meta['SourceURL'] = _config['url']
        write_metadata('RCRAInfo_' + str(year), source_meta,
                       category=EXT_DIR, datatype='source')
    else:
        source_meta = read_source_metadata(paths, set_stewi_meta('RCRAInfo_' + year,
                                           EXT_DIR),
                                           force_JSON=True)['tool_meta']
        write_metadata('RCRAInfo_' + year, source_meta, datatype=datatype)


def generate_state_totals(year):
    totals = pd.read_csv(RCRA_DATA_PATH.joinpath('RCRA_state_totals.csv'))
    totals = totals.rename(columns={'Location Name': 'state_name'})
    totals = totals[['state_name', year]]
    totals['FlowAmount_kg'] = totals[year] * USton_kg
    totals.drop(labels=year, axis=1, inplace=True)
    state_codes = pd.read_csv(DATA_PATH.joinpath('state_codes.csv'),
                              usecols=['states', 'state_name'])
    totals = totals.merge(state_codes, on='state_name')
    totals = totals.rename(columns={'states': 'State'})
    filename = DATA_PATH.joinpath(f'RCRAInfo_{year}_StateTotals.csv')
    totals.to_csv(filename, index=False)

    # Update validationSets_Sources.csv
    date_created = time.strptime(time.ctime(os.path.getctime(filename)))
    date_created = time.strftime('%d-%b-%Y', date_created)
    validation_dict = {'Inventory': 'RCRAInfo',
                       #'Version': '',
                       'Year': year,
                       'Name': 'Trends Analysis',
                       'URL': 'https://rcrapublic.epa.gov/rcrainfoweb/action/modules/br/trends/view',
                       'Criteria': 'Location: State, Metric: Generation, '
                       'Generators To Include: All Generators Included In The NBR',
                       'Date Acquired': date_created,
                       }
    update_validationsets_sources(validation_dict, date_acquired=True)


def validate_state_totals(report_year, flowbyfacility):
    log.info('validating data against state totals')
    file_path = DATA_PATH.joinpath(f'RCRAInfo_{report_year}_StateTotals.csv')
    if file_path.is_file():
        totals = pd.read_csv(file_path, dtype={"FlowAmount_kg": float})
        # Rename cols to match reference format
        totals.rename(columns={'FlowAmount_kg': 'FlowAmount'}, inplace=True)
        # Validate waste generated against state totals, include only NBR data
        flowbyfacility['State'] = flowbyfacility['FacilityID'].str[0:2]
        flowbyfacility = apply_filters_to_inventory(flowbyfacility, 'RCRAInfo',
                                                    report_year,
                                                    ['National_Biennial_Report',
                                                     'imported_wastes',
                                                     'US_States_only'])
        validation_df = validate_inventory(flowbyfacility,
                                           totals, group_by=['State'])
        write_validation_result('RCRAInfo', report_year, validation_df)
    else:
        log.warning(f'validation file for RCRAInfo_{report_year} does not exist.')


def main(**kwargs):

    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Extract information from RCRAInfo.\
                        [B] Organize files for each year.\
                        [C] Create RCRAInfo for StEWI\
                        [D] Process state totals for validation',
                        type = str)

    parser.add_argument('-Y', '--Year', nargs= '+',
                        help = 'What RCRA Biennial Report year you want to '
                        'retrieve or generate for StEWI',
                        type = str,
                        default = None)

    parser.add_argument('-T', '--Tables', nargs = '+',
                        help = 'What RCRAInfo tables you want.\
                        Check:\
                        https://rcrainfopreprod.epa.gov/rcrainfo-help/\
                        application/publicHelp/index.htm',
                        required = False,
                        default = ['BR_REPORTING'])

    if len(kwargs) == 0:
        kwargs = vars(parser.parse_args())

    for year in kwargs['Year']:
        if int(year) % 2 == 0:
            raise stewi.exceptions.InventoryNotAvailableError(
                inv='RCRAInfo', year=year)
        # Adds sepcified Year to BR_REPORTING table
        if 'Tables' in kwargs:
            tables = kwargs['Tables'].copy()
            if 'BR_REPORTING' in kwargs['Tables']:
                tables[kwargs['Tables'].index('BR_REPORTING')] = 'BR_REPORTING' + '_' + year
        else:
            tables = ['BR_REPORTING_' + year]

        if kwargs['Option'] == 'A':
            """If issues in running this option to download the data, go to the
            specified url and find the BR_REPORTING_year.zip file and save to
            OUTPUT_PATH. Also requires HD_LU_WASTE_CODE.zip"""
            query = _config['queries']['Table_of_tables']
            download_and_extract_zip(tables, query)

        elif kwargs['Option'] == 'B':
            organize_br_reporting_files_by_year(kwargs['Tables'], year)

        elif kwargs['Option'] == 'C':
            Generate_RCRAInfo_files_csv(year)

        elif kwargs['Option'] == 'D':
            """State totals are compiled from the Trends Analysis website
            and stored as csv. New years will be added as data becomes
            available"""
            generate_state_totals(year)


if __name__ == '__main__':
    main()
