#!/usr/bin/env python

# TRI import and processing
# This script uses the TRI Basic National Data File.
# Data files:https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-data-files-calendar-years-1987-2016
# 2015 documentation on file format: https://www.epa.gov/sites/production/files/2016-11/documents/tri_basic_data_file_format_v15.pdf
# The format may change from yr to yr requiring code updates.
# This code has been tested for 2014.

import pandas as pd
from StandardizedReleaseAndWasteInventories import globals

# Set some metadata
TRIyear = '2016'
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
# Import in pieces grabbing main fields plus unique amount and basis of estimate fields
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

# Examine each to make sure they are right
# import_fug
# import_stack
# import_streamA
# import_streamB
# import_streamC
# import_streamD
# import_streamE
# import_streamF
# import_onsiteland
# import_onsiteother
# import_offsiteland
# import_offsiteother

keys = ['fug', 'stack', 'streamA', 'streamB', 'streamC', 'streamD', 'streamE', 'streamF', 'onsiteland', 'onsiteother',
        'offsiteland', 'offsiteother']

values = [import_fug, import_stack, import_streamA, import_streamB,
          import_streamC, import_streamD, import_streamE, import_streamF,
          import_onsiteland, import_onsiteother, import_offsiteland, import_offsiteother]


def dict_create(k, v):
    dictionary = dict(zip(k, v))
    return dictionary


import_dict = dict_create(keys, values)

# Create a dictionary that had the import fields for each release type to use in import process

# Import TRI file
tri_csv = data_dir + "US_" + TRIyear + "_v15/US_1_" + TRIyear + "_v15.txt"
# tri_csv = data_dir + "TRI_2016_US.csv" #TRI file .. do not include in repository folder
tri = pd.DataFrame()

fieldnames = ['FacilityID', 'State', 'NAICS', 'OriginalFlowID', 'Unit', 'Amount', 'Basis of Estimate']


def truncate_fields(long_fields, e):
    l = len(long_fields) - e
    short_field_names = long_fields[0:l]
    return short_field_names


fieldnamesshort = truncate_fields(fieldnames, 1)


# import_dict
def format_source_columns(d):
    for k, v in d.items():
        tri_part = pd.read_csv(tri_csv, sep='\t', header=0, usecols=v, error_bad_lines=False)
        #		tri_part = pd.read_csv(tri_csv,header=0,error_bad_lines=False)
        if k.startswith('offsite'):
            tri_part.columns = fieldnamesshort
        else:
            tri_part.columns = fieldnames
            return tri_part


tri_part = format_source_columns(import_dict)


# drop NA for Amount, but leave in zeros
def drop_null(part, col):
    tri_prt = part.dropna(subset=[col])
    return tri_prt


tri_part1 = drop_null(tri_part, 'Amount')


# Set 'Source' to key for name of datatype
def set_source_column(d, prt, col):
    for k, v in d.items():
        prt[col] = k
        return prt


tri_part2 = set_source_column(import_dict, tri_part1, 'Source')


def combine_df_and_columns(df1, df2):
    result = pd.concat([df1, df2])
    return result


tri_part3 = combine_df_and_columns(tri, tri_part2)


# There is white space after some basis of estimate codes...remove it here
def strip_coln_white_space(df, coln):
    df[coln] = df[coln].str.strip()
    return df


tri_part4 = strip_coln_white_space(tri_part3, 'Basis of Estimate')


def convert_to_float(df, coln, err):
    df[coln] = pd.to_numeric(df[coln], errors=err)
    return df


tri_part5 = convert_to_float(tri_part4, 'Amount', 'coerce')


def step_output(df, directory, file_name):
    result = df.to_csv(directory + file_name)


step1 = step_output(tri_part5, output_dir, '1_trifirststep.csv')


# Show first 50 to see

# Import reliability scores for TRI
def import_file(path, coln):
    table = pd.read_csv(path, usecols=coln)
    return table


DQ_rel_path = data_dir + 'DQ_Reliability_Scores_Table3-3fromERGreport.csv'
read_coln_rel = ['Source', 'Code', 'DQI Reliability Score']

reliabilitytable = import_file(DQ_rel_path, read_coln_rel)


def redefine_columns(df, old_column, new_column):
    df[df[old_column] == new_column]
    return df


trireliabilitytable = redefine_columns(reliabilitytable, 'Source', 'TRI')


def drop_columns(df, coln):
    df.drop(coln, axis=1, inplace=True)
    return df


tri_rel_table = drop_columns(trireliabilitytable, 'Source')


# Link Basis of Estimate to Data Reliability Scores
def merge_tris(df1, df2, field1, field2, join_type):
    result = pd.merge(df1, df2, left_on=field1, right_on=field2, how=join_type)
    return result


tri_part6 = merge_tris(tri_part5, tri_rel_table, 'Basis of Estimate', 'Code', 'left')


# Fill NAs with 5 for DQI reliability score
def fill_null(df, coln, val):
    df[coln] = df[coln].fillna(value=val)
    return df


tri_part7 = fill_null(tri_part6, 'DQI Reliability Score', 5)
# Drop unneeded columns
tri_part8 = drop_columns(tri_part7, 'Basis of Estimate')
tri_part9 = drop_columns(tri_part8, 'Code')

# Drop unneeded columns
# Export for review
step2 = step_output(tri_part9, output_dir, '2_triafterreliabilitymerge.csv')

# Replace source info with Context
source_cnxt = data_dir + 'TRI_Source_to_Context.csv'
source_to_context = import_file(source_cnxt, all)
tri_part10 = pd.merge(tri_part9, source_to_context)

# Import pollutant omit list and use it to remove the pollutants to omit
omitlist_dir = data_dir + 'TRI_pollutant_omit_list.csv'
omitlist = import_file(omitlist_dir, all)

omitIDs = omitlist['OriginalFlowID']

# !Still need to implement code for this removal.
# Convert units to ref mass unit of kg
# Create a new field to put converted amount in
tri_part10['Amount_kg'] = 0.0


# Convert amounts. Note this could be replaced with a conversion utility
def unit_convert(df, coln1, coln2, unit, conversion_factor, coln3):
    df[coln1][df[coln2] == unit] = conversion_factor * df[coln3]
    return df


tri_part11 = unit_convert(tri_part10, 'Amount_kg', 'Unit', 'Pounds', 0.4535924, 'Amount')
tri_part12 = unit_convert(tri_part11, 'Amount_kg', 'Unit', 'Grams', 0.001, 'Amount')
step3 = step_output(tri_part12, output_dir, '3_triwithamountsconvertedshowingoldunits.csv')

# drop old amount and units
tri_part13 = drop_columns(tri_part12, 'Amount')
tri_part14 = drop_columns(tri_part13, 'Unit')


# Final cleanup - first rename then reorder
def rename_columns(df, old_column, new_column):
    df.rename(columns={old_column: new_column}, inplace=True)
    return df


tri_part15 = rename_columns(tri_part14, 'Amount_kg', 'Amount')
tri_part16 = rename_columns(tri_part15, 'DQI Reliability Score', 'ReliabilityScore')

step4 = step_output(tri_part14, output_dir, '4_triwithamountsconverted.csv')

# See final names and ordering from reference list
ref_list = data_dir + 'Standarized_Output_Format_EPA _Data_Sources.csv'
reflist = import_file(ref_list, all)
reflist = reflist[reflist['required?'] == 1]
refnames = list(reflist['Name'])
refnames.append('Source')

# Reorder columns to match standard format
tri_final = tri_part16.reindex(columns=refnames)

# !Save as R dataframe. Still tbd
# Use rpy2  package

# Export it as a csv
# Final file name
tri_file_name = 'TRI_' + TRIyear + '.csv'
tri_final.to_csv(output_dir + tri_file_name, index=False)
