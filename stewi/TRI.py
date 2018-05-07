#!/usr/bin/env python

# TRI import and processing
# This script uses the TRI Basic Plus National Data File.
# Data files:https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-calendar-years-1987-2016
# Documentation on file format: https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-guides
# The format may change from yr to yr requiring code updates.
# This code has been tested for 2014.

import pandas as pd
from stewi import globals
from stewi.globals import unit_convert

# Set some metadata
TRIyear = '2011'
output_dir = globals.output_dir
data_dir = globals.data_dir


# Import list of fields from TRI that are desired for LCI
def imp_fields(tri_fields_txt):
    tri_required_fields_csv = tri_fields_txt
    tri_req_fields = pd.read_table(tri_required_fields_csv, header=None)
    tri_req_fields = list(tri_req_fields[0])
    return tri_req_fields

tri_required_fields = (imp_fields(data_dir + 'TRI_required_fields.txt'))

# Import in pieces grabbing main fields plus unique amount and basis of estimate fields
# assigns fields to variables
def concat_req_field(a, b):
    source_name = tri_required_fields[0:5] + tri_required_fields[a:b]
    return source_name

import_fug = concat_req_field(5, 7)
import_stack = concat_req_field(7, 9)
import_streamA = concat_req_field(9, 11)
import_streamB = concat_req_field(11, 13)
import_streamC = concat_req_field(13, 15)
import_streamD = concat_req_field(15, 17)
import_streamE = concat_req_field(17, 19)
import_streamF = concat_req_field(19, 21)
import_onsiteland = concat_req_field(21, 23)
import_onsiteother = concat_req_field(23, 25)
# Offsite treatment does not include basis of estimate codes
import_offsiteland = concat_req_field(25, 26)
import_offsiteother = concat_req_field(26, 27)

keys = ['fug', 'stack', 'streamA', 'streamB', 'streamC', 'streamD', 'streamE', 'streamF', 'onsiteland', 'onsiteother',
        'offsiteland', 'offsiteother']

values = [import_fug, import_stack, import_streamA, import_streamB,
          import_streamC, import_streamD, import_streamE, import_streamF,
          import_onsiteland, import_onsiteother, import_offsiteland, import_offsiteother]

def dict_create(k, v):
    dictionary = dict(zip(k, v))
    return dictionary

# Create a dictionary that had the import fields for each release type to use in import process
import_dict = dict_create(keys, values)

# Import TRI file
tri_csv = '../TRI/US_' + TRIyear + '_v15/US_1_' + TRIyear + '_v15.txt'

fieldnames = ['FacilityID', 'State', 'NAICS', 'FlowName', 'Unit', 'FlowAmount','Basis of Estimate','ReleaseType']

# Cycle through file importing by release type, the dictionary key
def import_TRI_by_release_type(d):
    tri = pd.DataFrame()
    for k, v in d.items():
        tri_part = pd.read_csv(tri_csv, sep='\t', header=0, usecols=v, error_bad_lines=False)
        #print(k + ', ' + str(v))
        tri_part['ReleaseType'] = k
        if k.startswith('offsite'):
            tri_part['Basis of Estimate'] = 'NA'
        tri_part.columns = fieldnames
        tri = pd.concat([tri,tri_part])
    return tri

tri = import_TRI_by_release_type(import_dict)
#len(tri) = 953004 for 2016

# drop NA for Amount, but leave in zeros
tri = tri.dropna(subset=['FlowAmount'])

# There is white space after some basis of estimate codes...remove it here
def strip_coln_white_space(df, coln):
    df[coln] = df[coln].str.strip()
    return df

tri = strip_coln_white_space(tri, 'Basis of Estimate')

#Convert to float
tri['FlowAmount'] = pd.to_numeric(tri['FlowAmount'], errors='coerce')
#Drop 0 for FlowAmount
tri = tri[tri['FlowAmount'] != 0]
#len(tri) = 522700 for 2016

# Import reliability scores for TRI
reliability_table = globals.reliability_table
tri_reliability_table = reliability_table[reliability_table['Source']=='TRI']
tri_reliability_table.drop('Source', axis=1, inplace=True)

#Merge with reliability table to get
tri = pd.merge(tri,tri_reliability_table,left_on='Basis of Estimate',right_on='Code',how='left')
# Fill NAs with 5 for DQI reliability score
tri['DQI Reliability Score'] = tri['DQI Reliability Score'].fillna(value=5)
# Drop unneeded columns
#tri.drop('Note',axis=1,inplace=True)
tri.drop('Basis of Estimate',axis=1,inplace=True)
tri.drop('Code',axis=1,inplace=True)

# Replace source info with Context
source_cnxt = data_dir + 'TRI_ReleaseType_to_Compartment.csv'
source_to_context = pd.read_csv(source_cnxt)
tri = pd.merge(tri, source_to_context, how='left')

# Convert units to ref mass unit of kg
# Create a new field to put converted amount in
tri['Amount_kg'] = 0.0
tri = unit_convert(tri, 'Amount_kg', 'Unit', 'Pounds', 0.4535924, 'FlowAmount')
tri = unit_convert(tri, 'Amount_kg', 'Unit', 'Grams', 0.001, 'FlowAmount')
# drop old amount and units
tri.drop('FlowAmount',axis=1,inplace=True)
tri.drop('Unit',axis=1,inplace=True)

# Rename cols to match reference format
tri.rename(columns={'Amount_kg':'FlowAmount'},inplace=True)
tri.rename(columns={'DQI Reliability Score':'ReliabilityScore'},inplace=True)

# Export it as a csv
tri_file_name = 'TRI_' + TRIyear + '.csv'
tri.to_csv(output_dir + tri_file_name, index=False)
