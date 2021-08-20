# TRI.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Downloads TRI Basic Plus files specified in paramaters for specified year
This file requires parameters be passed like:
    Option -Y Year -F File1 File2 ... FileN
    where Option is either A, B, C:
Options
    A - for downloading and extracting files from TRI Data Plus web site
    B - for organizing TRI National Totals files from TRI_chem_release_Year.csv
    (this is expected to be download before and to be organized as it is
    described in TRI.py).
    C - for generating StEWI output files and validation from downloaded data
Year is like 2010 with coverage up to 2018
Files are:
    1a - Releases and Other Waste Mgmt
    3a - Off Site Transfers
See more documentation of files at 
https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-guides

Years:
    2008 through 2019

"""

import requests
import zipfile
from bs4 import BeautifulSoup
import pandas as pd
import time
import os.path, os, io, sys
import argparse
import re

from esupy.processed_data_mgmt import create_paths_if_missing
from stewi.globals import unit_convert,data_dir, set_stewi_meta,\
    get_reliability_table_for_source,validate_inventory,\
    write_validation_result,write_metadata,url_is_alive,\
    lb_kg,g_kg,config,store_inventory,log, paths, compile_source_metadata,\
    read_source_metadata, update_validationsets_sources, aggregate


ext_folder = 'TRI Data Files'
tri_external_dir = paths.local_path + '/' + ext_folder + '/'
_config = config()['databases']['TRI']
tri_data_dir = data_dir + 'TRI/'

def visit(url):
    html  = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    return soup


def link_zip(url, queries, year):
    soup = visit(url)
    TRI_zip_options = {}
    for link in soup.find_all(queries['TRI_year_reported']):
        TRI_zip_options[link.text] = link.get(queries['TRI_zip'])
    return TRI_zip_options[year]


def extract_TRI_data_files(link_zip, files, year):
    r_file = requests.get(link_zip)
    for file in files:
        df_columns = pd.read_csv(tri_data_dir + 'TRI_File_' +\
                                 file + '_columns.txt', header = 0)
        columns = list(df_columns['Names'])
        n_columns = len(columns)
        filename = 'US_' + file + '_' + year
        dic = {}
        i = 0
        with zipfile.ZipFile(io.BytesIO(r_file.content)) as z:
            with io.TextIOWrapper(z.open(filename + '.txt',
                                         mode='r'),) as txtfile:
                for line in txtfile:
                    dic[i] = pd.Series(re.split("\t",line)).truncate(after=n_columns-1)
                    i+=1
        # remove the first row in the dictionary which is the original headers
        del dic[0]
        df = pd.DataFrame.from_dict(dic, orient='index')
        df.columns = columns
        create_paths_if_missing(tri_external_dir)
        df.to_csv(tri_external_dir + filename + '.csv', index = False)
        log.info(filename + '.csv saved to ' + tri_external_dir)


def generate_national_totals(year):
    """Generate dataframe of national emissions and save to csv. Requires the
    chem_release dataset to be downloaded manually prior to running"""
    filename = tri_data_dir + 'TRI_chem_release_' + year + '.csv'
    df = pd.read_csv(filename, header = 0)
    df.replace(',', 0.0, inplace = True)
    df.replace('.', 0.0, inplace = True)
    cols = ['Compartment', 'FlowName', 'Unit', 'FlowAmount']
    compartments = {'air': ['Fugitive Air Emissions',
                            'Point Source Air Emissions'],
                    'water': ['Surface Water Discharges'],
                    'soil': ['On-site Land Treatment',
                             'Other On-site Land Disposal',
                             'Off-site Land Treatment',
                             'Other Off-site Land Disposal']}
    # remove entries where all values are 0
    v = [col for col in df.columns if col != 'Chemical']
    df = df.loc[~(df[v]==0).all(axis=1)]
    df_National = pd.DataFrame()
    for compartment, columns in compartments.items():
        df_aux = df[['Chemical'] + columns].reset_index(drop=True)
        for column in columns:
            df_aux[column] = df_aux[column].str.replace(',','').astype('float')
        df_aux['FlowAmount'] = df_aux[columns].sum(axis = 1)
        df_aux.rename(columns = {'Chemical': 'FlowName'}, inplace = True)
        df_aux['Unit'] = 'Pounds'
        df_aux['Compartment'] = compartment
        df_National = pd.concat([df_National, df_aux], axis = 0,
                                ignore_index = True,
                                sort = True)
        del df_aux
    del df
    df_National['FlowAmount'] = df_National['FlowAmount'].round(3)
    df_National = df_National[cols]
    df_National = map_to_fedefl(df_National)
    if df_National is None:
        log.warning('Totals not generated')
        return
    df_National.sort_values(by=['FlowName','Compartment'], inplace=True)
    log.info('saving TRI_%s_NationalTotals.csv to %s', year, data_dir)
    df_National.to_csv(data_dir + 'TRI_' + year + '_NationalTotals.csv',
                       index = False)
    
    # Update validationSets_Sources.csv
    date_created = time.strptime(time.ctime(os.path.getctime(filename)))
    date_created = time.strftime('%d-%b-%Y', date_created)
    validation_dict = {'Inventory':'TRI',
                       #'Version':'',
                       'Year':year,
                       'Name':'TRI Explorer',
                       'URL':'https://enviro.epa.gov/triexplorer/tri_release.chemical',
                       'Criteria':'Year, All of United States, All Chemicals, '
                       'All Industries, Details:(Other On-Site Disposal or '
                       'Other Releases, Other Off-Site Disposal or Other Releases), '
                       'mapped to FEDEFL',
                       'Date Acquired':date_created,
                       }
    update_validationsets_sources(validation_dict, date_acquired=True)

def map_to_fedefl(df):
    try:
        import fedelemflowlist
    except ImportError:
        log.warning('requires installation of fedelemflowlist, data will not '
                    'validate correctly')
        return None
    tri = fedelemflowlist.get_flowmapping('TRI')
    tri = tri[['SourceFlowName', 'TargetFlowName']].drop_duplicates()
    mapped_df = df.merge(tri, how = 'left', left_on = 'FlowName',
                                   right_on = 'SourceFlowName')
    if mapped_df['FlowAmount'].sum() != df['FlowAmount'].sum():
        log.warning('Error on mapping, data loss')
    # validation throws errors when mixture and trade secret chemicals are 
    # maintained so drop them while they remain unmapped
    criteria = (mapped_df['TargetFlowName'].isna() &
                (mapped_df['FlowName'].str.lower().str.contains('trade secret') |
                mapped_df['FlowName'].str.lower().str.contains('mixture')))
    mapped_df = mapped_df[~criteria].reset_index(drop = True)
    missing_flows = mapped_df[mapped_df['TargetFlowName'].isna()]['FlowName']
    missing_flows = missing_flows.drop_duplicates().sort_values()
    if len(missing_flows) > 0:
        log.debug('flows from reference df missing in mapping file')
    mapped_df.loc[~mapped_df['TargetFlowName'].isna(),
                  'FlowName'] = mapped_df['TargetFlowName']
    mapped_df = mapped_df.drop(columns=['SourceFlowName','TargetFlowName'])
    return mapped_df


def imp_fields(tri_fields_txt):
    """
    Import list of fields from TRI that are desired for LCI
    """
    tri_required_fields_csv = tri_fields_txt
    tri_req_fields = pd.read_csv(tri_required_fields_csv, header=None)
    tri_req_fields = list(tri_req_fields[0])
    return tri_req_fields


def concat_req_field(list):
    """
    Import in pieces grabbing main fields plus unique amount and basis 
    of estimate fields assigns fields to variables
    """
    source_name = ['TRIFID','CHEMICAL NAME', 'CAS NUMBER',
                   'UNIT OF MEASURE'] + list
    return source_name


def dict_create(k, v):
    dictionary = dict(zip(k, v))
    return dictionary


# Cycle through file importing by release type, the dictionary key
def import_TRI_by_release_type(d, year):
    # Import TRI file
    tri_release_output_fieldnames = ['FacilityID', 'CAS', 'FlowName',
                                     'Unit', 'FlowAmount','Basis of Estimate',
                                     'ReleaseType']
    tri = pd.DataFrame()
    for k, v in d.items():
        #create a data type dictionary
        dtype_dict = {'TRIFID':"str", 'CHEMICAL NAME':"str",
                      'CAS NUMBER':"str",'UNIT OF MEASURE':"str"}
        #If a basis of estimate field is present, set its type to string
        if len(v) > 5:
            dtype_dict[v[5]] = "str"
        if (k == 'offsiteland') | (k == 'offsiteother'):
            file = '3a'
        else:
            file = '1a'
        tri_csv = tri_external_dir + 'US_' + file + '_' + year + '.csv'
        try:
            tri_part = pd.read_csv(tri_csv, usecols = v,
                                   low_memory = False)
            tri_part['ReleaseType'] = k
            tri_part.columns = tri_release_output_fieldnames
            tri = pd.concat([tri,tri_part])
        except FileNotFoundError:
            log.error('%s.csv file not found in %s', file, tri_csv)
    if len(tri)==0:
        log.error('No data found. Please run option A before proceeding')
        sys.exit(0)
    return tri

# There is white space after some basis of estimate codes...remove it here
def strip_coln_white_space(df, coln):
    df[coln] = df[coln].str.strip()
    return df


def validate_national_totals(inv, TRIyear):
    #VALIDATE
    log.info('validating data against national totals')
    if (os.path.exists(data_dir + 'TRI_'+ TRIyear + '_NationalTotals.csv')):
        tri_national_totals = pd.read_csv(data_dir + 'TRI_'+ TRIyear +\
                                          '_NationalTotals.csv',
                                          header=0,dtype={"FlowAmount":float})
        tri_national_totals['FlowAmount_kg']=0
        tri_national_totals = unit_convert(tri_national_totals, 'FlowAmount_kg',
                                           'Unit', 'Pounds', lb_kg, 'FlowAmount')
        # drop old amount and units
        tri_national_totals.drop('FlowAmount',axis=1,inplace=True)
        tri_national_totals.drop('Unit',axis=1,inplace=True)
        # Rename cols to match reference format
        tri_national_totals.rename(columns={'FlowAmount_kg':'FlowAmount'},
                                   inplace=True)
        inv = map_to_fedefl(inv)
        if inv is not None:
            validation_result = validate_inventory(inv, tri_national_totals,
                                                   group_by='flow', tolerance=5.0)
            write_validation_result('TRI',TRIyear,validation_result)
    else:
        log.warning('validation file for TRI_%s does not exist. Please run '
                    'option B', TRIyear)

def Generate_TRI_files_csv(TRIyear, Files):
    """Generate TRI inventories from downloaded files"""
    tri_required_fields = imp_fields(tri_data_dir + 'TRI_required_fields.txt')
    keys = imp_fields(tri_data_dir + 'TRI_keys.txt') # the same function can be used
    values = list()
    for p in range(len(keys)):
        start = 13 + 2*p
        end =  start + 1
        values.append(concat_req_field(tri_required_fields[start:end + 1]))
    # Create a dictionary that had the import fields for each release 
    # type to use in import process
    import_dict = dict_create(keys, values)
    # Build the TRI DataFrame
    tri = import_TRI_by_release_type(import_dict, TRIyear)
    # drop NA for Amount, but leave in zeros
    tri = tri.dropna(subset=['FlowAmount'])
    tri = strip_coln_white_space(tri, 'Basis of Estimate')
    #Convert to float if there are errors - be careful with this line
    if tri['FlowAmount'].values.dtype != 'float64':
        tri['FlowAmount'] = pd.to_numeric(tri['FlowAmount'], errors = 'coerce')
    #Drop 0 for FlowAmount
    tri = tri[tri['FlowAmount'] != 0]
    # Import reliability scores for TRI
    tri_reliability_table = get_reliability_table_for_source('TRI')
    #Merge with reliability table to get
    tri = pd.merge(tri,tri_reliability_table,left_on='Basis of Estimate',
                   right_on='Code',how='left')
    # Fill NAs with 5 for DQI reliability score
    tri['DQI Reliability Score'] = tri['DQI Reliability Score'].fillna(value=5)
    # Drop unneeded columns
    tri.drop(['Basis of Estimate', 'Code'],axis=1,inplace=True)
    # Replace source info with Context
    source_cnxt = tri_data_dir + 'TRI_ReleaseType_to_Compartment.csv'
    source_to_context = pd.read_csv(source_cnxt)
    tri = pd.merge(tri, source_to_context, how='left')
    # Convert units to ref mass unit of kg
    tri['Amount_kg'] = 0.0
    tri = unit_convert(tri, 'Amount_kg', 'Unit', 'Pounds', lb_kg, 'FlowAmount')
    tri = unit_convert(tri, 'Amount_kg', 'Unit', 'Grams', g_kg, 'FlowAmount')
    tri.drop(['FlowAmount', 'Unit'],axis=1,inplace=True)
    # Rename cols to match reference format
    tri.rename(columns={'Amount_kg':'FlowAmount',
                        'DQI Reliability Score':'DataReliability'},
               inplace=True)
    #Drop release type
    tri.drop('ReleaseType',axis=1,inplace=True)
    #Group by facility, flow and compartment to aggregate different release types
    grouping_vars = ['FacilityID', 'FlowName','CAS','Compartment']
    tri = aggregate(tri, grouping_vars)

    validate_national_totals(tri, TRIyear)
    
    #FLOWS
    flowsdf = tri[['FlowName','CAS','Compartment']
                  ].drop_duplicates().reset_index(drop=True)
    flowsdf.loc[:,'FlowID'] = flowsdf['CAS']
    store_inventory(flowsdf, 'TRI_' + TRIyear, 'flow')
    
    #FLOW BY FACILITY
    tri.drop(columns=['CAS'],inplace=True)
    store_inventory(tri, 'TRI_' + TRIyear, 'flowbyfacility')
    
    #FACILITY
    ##Import and handle TRI facility data
    import_facility = tri_required_fields[0:10]
    tri_facility = pd.read_csv(tri_external_dir + 'US_1a_' + TRIyear + '.csv',
                                    usecols=import_facility,
                                    low_memory = False)
    #get unique facilities
    tri_facility  = tri_facility.drop_duplicates(ignore_index = True)
    #rename columns
    TRI_facility_name_crosswalk = {
                                'TRIFID':'FacilityID',
                                'FACILITY NAME':'FacilityName',
                                'FACILITY STREET':'Address',
                                'FACILITY CITY':'City',
                                'FACILITY COUNTY':'County',
                                'FACILITY STATE': 'State',
                                'FACILITY ZIP CODE':'Zip',
                                'PRIMARY NAICS CODE':'NAICS',
                                'LATITUDE': 'Latitude',
                                'LONGITUDE':'Longitude'
                                  }
    tri_facility.rename(columns=TRI_facility_name_crosswalk,
                              inplace=True)
    store_inventory(tri_facility, 'TRI_' + TRIyear, 'facility')


def generate_metadata(year, files, datatype = 'inventory'):
    """
    Gets metadata and writes to .json
    """
    if datatype == 'source':
        source_path = [tri_external_dir + 'US_' + p + '_' + \
                       year + '.csv' for p in files]
        source_path = [os.path.realpath(p) for p in source_path]
        source_meta = compile_source_metadata(source_path, _config, year)
        source_meta['SourceType'] = 'Zip file'
        tri_url = _config['url']
        link_zip_TRI = link_zip(tri_url, _config['queries'], year)
        regex = re.compile(r'https://www3.epa.gov/tri/current/US_\d{4}_?(\d*)\.zip')
        tri_version = re.search(regex, link_zip_TRI).group(0)
        if not tri_version:
            tri_version = 'last'
        source_meta['SourceVersion'] = tri_version
        write_metadata('TRI_'+year, source_meta, category=ext_folder,
                       datatype='source')
    else:
        source_meta = read_source_metadata(paths, set_stewi_meta('TRI_'+ year, 
                                                          ext_folder),
                                           force_JSON=True)['tool_meta']
        write_metadata('TRI_'+year, source_meta, datatype=datatype)


def main(**kwargs):

    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Download and TRI flat files from TRI Data Plus.\
                        [B] Format national totals for TRI from download \
                        national files.\
                        [C] Generate StEWI inventory files from downloaded files',
                        type = str)

    parser.add_argument('-Y', '--Year', nargs = '+',
                        help = 'What TRI year you want to retrieve',
                        type = str)

    parser.add_argument('-F', '--Files', nargs = '+',
                        help = 'What TRI Files you want (e.g., 1a, 2a, etc).\
                        Check:\
                        https://www.epa.gov/toxics-release-inventory-tri-program/\
                            tri-basic-plus-data-files-guides',
                        default = ['1a','3a'],
                        required = False)

    if len(kwargs) == 0:
        kwargs = vars(parser.parse_args())

    TRIFiles = kwargs['Files']

    for year in kwargs['Year']:

        if kwargs['Option'] == 'A':
            log.info('downloading TRI files from source for %s', year)
            tri_url = _config['url']
            if url_is_alive(tri_url):
                link_zip_TRI = link_zip(tri_url, _config['queries'], year)
                extract_TRI_data_files(link_zip_TRI, TRIFiles, year)
                generate_metadata(year, TRIFiles, datatype='source')
            else:
                log.error('The URL in config.yaml ({}) for TRI is not '
                          'reachable.'.format(tri_url))

        elif kwargs['Option'] == 'B':
            # Website for National Totals
            # https://enviro.epa.gov/triexplorer/tri_release.chemical
            # Steps:
            # (1) Select Year of Data, All of United States, All Chemicals,
            # All Industry, and other needed option (this is based on the
            # desired year)
            # Columns: check 'Other On-site Disposal or Other Releases' and
            # 'Other Off-site Disposal or Other Releases'
            # (2) Export to CSV
            # (3) Drop the not needed rows, including the extra dioxin row 
            # at the bottom
            # (4) Organize the columns as they are needed (check existing files)
            # (5) Save the file like TRI_chem_release_year.csv in data folder
            # (6) Run this code

            generate_national_totals(year)

        elif kwargs['Option'] == 'C':
            log.info('generating TRI inventory from files for %s', year)
            Generate_TRI_files_csv(year, TRIFiles)
            generate_metadata(year, TRIFiles, datatype='inventory')

if __name__ == '__main__':
    main()
