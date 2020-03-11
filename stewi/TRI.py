#!/usr/bin/env python
"""
Downloads TRI Basic Plus files specified in paramaters for specified year
This file requires parameters be passed like:
Option Year -F File1 File2 … FileN
where Option is either A, B, C:
Options
A - for extracting files from TRI Data Plus web site
B - for organizing TRI National Totals files from TRI_chem_release_Year.csv (this is expected to be download before and to be organized as it is described in TRI.py).
C - for organizing TRI as required by StEWI
Year is like 2010 with coverage up to 2018
Files are:
1a - Releases and Other Waste Mgmt
3a - Off Site Transfers
See more documentation of files at https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-guides
"""

import requests
import zipfile
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import os.path, os, io, sys
from stewi.globals import unit_convert,set_dir,output_dir,data_dir,reliability_table,inventory_metadata,\
    validate_inventory,write_validation_result,write_metadata,url_is_alive,get_relpath,lb_kg,g_kg,config
import argparse
import re

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


def extacting_TRI_data_files(link_zip, files, year):
    external_dir = set_dir(data_dir + '../../../')
    r_file = requests.get(link_zip)
    for file in files:
        df_columns = pd.read_csv(data_dir + '/TRI_File_' + file + '_columns.txt', header = 0)
        columns = list(df_columns['Names'])
        n_columns = len(columns)
        with zipfile.ZipFile(io.BytesIO(r_file.content)) as z:
            z.extract('US_' + file + '_' + year + '.txt' , external_dir + 'TRI')
        df = pd.read_csv(external_dir + 'TRI/US_' + file + '_' + year + '.txt',
                        header = None, encoding = 'ISO-8859-1',
                        error_bad_lines = False,
                        sep = '\t',
                        low_memory = False,
                        skiprows = [0],
                        lineterminator = '\n',
                        usecols = range(n_columns)) # avoiding \r\n created in Windows OS
        df.columns = columns
        df.to_csv(external_dir + 'TRI/US_' + file + '_' + year + '.txt',
                    sep = '\t', index = False)

# National Totals
def Generate_National_Total(year):
    df = pd.read_csv(data_dir + '/TRI_chem_release_' + year + '.csv', header = 0)
    cols = ['Compartment', 'FlowName', 'Unit', 'FlowAmount']
    df_National = pd.DataFrame(columns = cols)
    regex = re.compile(r'[\d\,]*\d+\.?\d*')
    for index, row in df.iterrows():
        df_aux = pd.DataFrame({'FlowName': [row[0]]*3, 'Unit': ['Pounds']*3,
                'Compartment': ['air', 'water', 'soil']})
        Amount = []
        for f in row[1:8]:
            if re.match(regex, f):
                Amount.append(float(f.replace(',','')))
            else:
                Amount.append(0.0)
        df_aux['FlowAmount'] = pd.Series([sum(Amount[0:2]), # air
                                Amount[2], # water
                                sum(Amount[3:7])]) # soil

        df_National = df_National.append(df_aux, ignore_index = True, sort = True)
    df_National['FlowAmount'] = df_National['FlowAmount'].round(3)
    df_National = df_National[cols]
    df_National.to_csv(data_dir + 'TRI_' + year + '_NationalTotals.csv', index = False)


# Function for calculating weighted average and avoiding ZeroDivisionError, which ocurres
# "when all weights along axis are zero".
def weight_mean(v, w):
    try:
        return np.average(v, weights = w)
    except ZeroDivisionError:
        return v.mean()

# Import list of fields from TRI that are desired for LCI
def imp_fields(tri_fields_txt):
    tri_required_fields_csv = tri_fields_txt
    tri_req_fields = pd.read_csv(tri_required_fields_csv, header=None)
    tri_req_fields = list(tri_req_fields[0])
    return tri_req_fields

# Import in pieces grabbing main fields plus unique amount and basis of estimate fields
# assigns fields to variables
def concat_req_field(list):
    source_name = ['TRIFID','CHEMICAL NAME', 'CAS NUMBER','UNIT OF MEASURE'] + list
    return source_name


def dict_create(k, v):
    dictionary = dict(zip(k, v))
    return dictionary


# Cycle through file importing by release type, the dictionary key
def import_TRI_by_release_type(d, year):
    # Import TRI file
    external_dir = set_dir(data_dir + '../../../')
    tri_release_output_fieldnames = ['FacilityID', 'CAS', 'FlowName', 'Unit', 'FlowAmount','Basis of Estimate','ReleaseType']
    tri = pd.DataFrame()
    for k, v in d.items():
        #create a data type dictionary
        dtype_dict = {'TRIFID':"str", 'CHEMICAL NAME':"str", 'CAS NUMBER':"str",'UNIT OF MEASURE':"str"}
        #If a basis of estimate field is present, set its type to string
        if len(v) > 5:
            dtype_dict[v[5]] = "str"
        if (k == 'offsiteland') | (k == 'offsiteother'):
            file = '3a'
        else:
            file = '1a'
        tri_csv = external_dir + 'TRI/US_' + file + '_' + year + '.txt'
        tri_part = pd.read_csv(tri_csv, sep='\t', header=0, usecols = v, dtype = dtype_dict, na_values = ['NO'],
                                error_bad_lines = False, low_memory = False,
                                converters = {v[4]: lambda x:  pd.to_numeric(x, errors = 'coerce')})

        tri_part['ReleaseType'] = k
        tri_part.columns = tri_release_output_fieldnames
        tri = pd.concat([tri,tri_part])
    return tri

# There is white space after some basis of estimate codes...remove it here
def strip_coln_white_space(df, coln):
    df[coln] = df[coln].str.strip()
    return df


def Generate_TRI_files_csv(TRIyear, Files):
    _config = config()['databases']['TRI']
    tri_url = _config['url']
    link_zip_TRI = link_zip(tri_url, _config['queries'], TRIyear)
    regex = re.compile(r'https://www3.epa.gov/tri/current/US_\d{4}_?(\d*)\.zip')
    tri_version = re.search(regex, link_zip_TRI).group(1)
    if not tri_version:
        tri_version = 'last'
    tri_required_fields = imp_fields(data_dir + 'TRI_required_fields.txt')
    keys = imp_fields(data_dir + 'TRI_keys.txt') # the same function can be used
    import_facility = tri_required_fields[0:10]
    values = list()
    for p in range(len(keys)):
        start = 13 + 2*p
        end =  start + 1
        values.append(concat_req_field(tri_required_fields[start:end + 1]))
    # Create a dictionary that had the import fields for each release type to use in import process
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
    tri_reliability_table = reliability_table[reliability_table['Source']=='TRI']
    tri_reliability_table.drop('Source', axis=1, inplace=True)
    #Merge with reliability table to get
    tri = pd.merge(tri,tri_reliability_table,left_on='Basis of Estimate',right_on='Code',how='left')
    # Fill NAs with 5 for DQI reliability score
    tri['DQI Reliability Score'] = tri['DQI Reliability Score'].fillna(value=5)
    # Drop unneeded columns
    tri.drop('Basis of Estimate',axis=1,inplace=True)
    tri.drop('Code',axis=1,inplace=True)
    # Replace source info with Context
    source_cnxt = data_dir + 'TRI_ReleaseType_to_Compartment.csv'
    source_to_context = pd.read_csv(source_cnxt)
    tri = pd.merge(tri, source_to_context, how='left')
    # Convert units to ref mass unit of kg
    # Create a new field to put converted amount in
    tri['Amount_kg'] = 0.0
    tri = unit_convert(tri, 'Amount_kg', 'Unit', 'Pounds', lb_kg, 'FlowAmount')
    tri = unit_convert(tri, 'Amount_kg', 'Unit', 'Grams', g_kg, 'FlowAmount')
    # drop old amount and units
    tri.drop('FlowAmount',axis=1,inplace=True)
    tri.drop('Unit',axis=1,inplace=True)
    # Rename cols to match reference format
    tri.rename(columns={'Amount_kg':'FlowAmount'}, inplace=True)
    tri.rename(columns={'DQI Reliability Score':'ReliabilityScore'}, inplace=True)
    #Drop release type
    tri.drop('ReleaseType',axis=1,inplace=True)
    #Group by facility, flow and compartment to aggregate different release types
    grouping_vars = ['FacilityID', 'FlowName','CAS','Compartment']
    wm = lambda x: weight_mean(x, tri.loc[x.index, "FlowAmount"])
    # Define a dictionary with the functions to apply for a given column:
    f = {'FlowAmount': ['sum'], 'ReliabilityScore': {'weighted_mean': wm}}
    # Groupby and aggregate with your dictionary:
    tri = tri.groupby(grouping_vars).agg(f)
    tri = tri.reset_index()
    tri.columns = tri.columns.droplevel(level=1)
    #VALIDATE
    tri_national_totals = pd.read_csv(data_dir + 'TRI_'+ TRIyear + '_NationalTotals.csv',header=0,dtype={"FlowAmount":np.float})
    tri_national_totals['FlowAmount_kg']=0
    tri_national_totals = unit_convert(tri_national_totals, 'FlowAmount_kg', 'Unit', 'Pounds', 0.4535924, 'FlowAmount')
    # drop old amount and units
    tri_national_totals.drop('FlowAmount',axis=1,inplace=True)
    tri_national_totals.drop('Unit',axis=1,inplace=True)
    # Rename cols to match reference format
    tri_national_totals.rename(columns={'FlowAmount_kg':'FlowAmount'},inplace=True)
    validation_result = validate_inventory(tri, tri_national_totals, group_by='flow', tolerance=5.0)
    write_validation_result('TRI',TRIyear,validation_result)
    #FLOWS
    flows = tri.groupby(['FlowName','CAS','Compartment']).count().reset_index()
    #stack by compartment
    flowsdf = flows[['FlowName','CAS','Compartment']]
    flowsdf['FlowID'] = flowsdf['CAS']
    #export chemicals
    #!!!Still needs CAS number and FlowID
    flowsdf.to_csv(output_dir+'flow/'+'TRI_'+ TRIyear + '.csv', index=False)
    #FLOW BY FACILITY
    #drop CAS
    tri.drop(columns=['CAS'],inplace=True)
    tri_file_name = 'TRI_' + TRIyear + '.csv'
    tri.to_csv(output_dir + 'flowbyfacility/' + tri_file_name, index=False)
    #FACILITY
    ##Import and handle TRI facility data
    tri_facility = pd.read_csv(set_dir(data_dir + '../../../') + 'TRI/US_1_' + TRIyear + '.txt',
                                    sep='\t', header=0, usecols=import_facility,
                                    error_bad_lines=False,
                                    low_memory = False)
    #get unique facilities
    tri_facility_unique_ids = pd.unique(tri_facility['TRIFID'])
    tri_facility_unique_rows  = tri_facility.drop_duplicates()
    #Use group by to elimiate additional ID duplicates
    #tri_facility_unique_rows_agg = tri_facility_unique_rows.groupby(['TRIFID'])
    #tri_facility_final = tri_facility_unique_rows_agg.aggregate()
    tri_facility_final = tri_facility_unique_rows
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
    tri_facility_final.rename(columns=TRI_facility_name_crosswalk,inplace=True)
    tri_facility_final.to_csv(output_dir+'facility/'+'TRI_'+ TRIyear + '.csv', index=False)
    # Record TRI metadata
    external_dir = set_dir(data_dir + '../../../')
    for file in Files:
        tri_csv = external_dir + 'TRI/US_' + file + '_' + TRIyear + '.txt'
        try: retrieval_time = os.path.getctime(tri_csv)
        except: retrieval_time = time.time()
        tri_metadata['SourceAquisitionTime'] = time.ctime(retrieval_time)
        tri_metadata['SourceFileName'] = get_relpath(tri_csv)
        tri_metadata['SourceURL'] = tri_url
        tri_metadata['SourceVersion'] = tri_version
        write_metadata('TRI', TRIyear, tri_metadata)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Extract TRI flat files from TRI Data Plus.\
                        [B] National Totals for TRI.\
                        [C] Organize files',
                        type = str)

    parser.add_argument('Year',
                        help = 'What TRI year you want to retrieve',
                        type = str)

    parser.add_argument('-F', '--Files', nargs = '+',
                        help = 'What TRI Files you want (e.g., 1a, 3a, etc).\
                        Check:\
                        https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-guides',
                        required = False)

    args = parser.parse_args()

    # Set some metadata
    TRIyear = args.Year
    tri_metadata = inventory_metadata

    if args.Option == 'A':

        config = config()['databases']['TRI']
        tri_url = config['url']
        if url_is_alive(tri_url):
            link_zip_TRI = link_zip(tri_url, config['queries'], TRIyear)
            extacting_TRI_data_files(link_zip_TRI, args.Files, TRIyear)
        else:
            print('The URL in config.yaml ({}) for TRI is not reachable.'.format(tri_url))

    elif args.Option == 'B':

        # Website for National Totals
        # https://iaspub.epa.gov/triexplorer/tri_release.chemical (3/17/2019)
        # Steps:
        # (1) Select Year of Data, All of United States, All Chemicals, All Industry,
        #  and other needed option (this is based on the desired year)
        # (2) Export to CSV
        # (3) Drop the not needed rows
        # (4) Organize the columns as they are needed (check excisting files)
        # (5) Save the file like TRI_chem_release_year.csv in data folder
        # (6) Run this code

        Generate_National_Total(TRIyear)

    elif args.Option == 'C':

        Generate_TRI_files_csv(TRIyear, args.Files)
