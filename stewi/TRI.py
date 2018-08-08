#!/usr/bin/env python

# TRI import and processing
# This script uses the TRI Basic Plus National Data File.
# Data files:https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-calendar-years-1987-2016
# Documentation on file format: https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-guides

import pandas as pd
import numpy as np
import time
import os.path
from stewi.globals import unit_convert,set_dir,output_dir,data_dir,reliability_table,inventory_metadata,\
    validate_inventory,write_validation_result,write_metadata,url_is_alive,get_relpath,lb_kg,g_kg

import logging

# Set some metadata
TRIyear = '2016'
tri_metadata = inventory_metadata
tri_url = 'https://www3.epa.gov/tri/current/US_' + TRIyear + '_v'


def get_current_version():
    version = 15# Most recent version of TRI Basic Plus as of writing this is v15
    while url_is_alive(tri_url + str(int(version + 1)) + '.zip'): version += 1
    return str(version)


# Import list of fields from TRI that are desired for LCI
def imp_fields(tri_fields_txt):
    tri_required_fields_csv = tri_fields_txt
    tri_req_fields = pd.read_table(tri_required_fields_csv, header=None)
    tri_req_fields = list(tri_req_fields[0])
    return tri_req_fields


tri_version = get_current_version()
tri_url += tri_metadata['SourceVersion'] + '.zip'
tri_required_fields = (imp_fields(data_dir + 'TRI_required_fields.txt'))


# Import in pieces grabbing main fields plus unique amount and basis of estimate fields
# assigns fields to variables
def concat_req_field(list):
    source_name = ['TRIFID','CHEMICAL NAME', 'CAS NUMBER','UNIT OF MEASURE'] + list
    return source_name


facility_fields = ['FACILITY NAME','FACILITY STREET','FACILITY CITY','FACILITY COUNTY','FACILITY STATE',
                   'FACILITY ZIP CODE','PRIMARY NAICS CODE','LATITUDE','LONGITUDE']

fug_fields = ['TOTAL FUGITIVE AIR EMISSIONS','FUGITIVE OR NON-POINT AIR EMISSIONS - BASIS OF ESTIMATE']
stack_fields = ['TOTAL STACK AIR EMISSIONS','STACK OR POINT AIR EMISSIONS - BASIS OF ESTIMATE']
streamA_fields = ['TOTAL DISCHARGES TO STREAM A','DISCHARGES TO STREAM A - BASIS OF ESTIMATE']
streamB_fields = ['TOTAL DISCHARGES TO STREAM B','DISCHARGES TO STREAM B - BASIS OF ESTIMATE']
streamC_fields = ['TOTAL DISCHARGES TO STREAM C','DISCHARGES TO STREAM C - BASIS OF ESTIMATE']
streamD_fields = ['TOTAL DISCHARGES TO STREAM D','DISCHARGES TO STREAM D - BASIS OF ESTIMATE']
streamE_fields = ['TOTAL DISCHARGES TO STREAM E','DISCHARGES TO STREAM E - BASIS OF ESTIMATE']
streamF_fields = ['TOTAL DISCHARGES TO STREAM F','DISCHARGES TO STREAM F - BASIS OF ESTIMATE']
onsiteland_fields = ['TOTAL LAND TREATMENT','LAND TRTMT/APPL FARMING - BASIS OF ESTIMATE']
onsiteother_fields = ['TOTAL OTHER DISPOSAL','OTHER DISPOSAL -BASIS OF ESTIMATE']
offsiteland_fields  = ['LAND TREATMENT']
offsiteother_fields  = ['OTHER LAND DISPOSAL']

import_facility = ['TRIFID'] + facility_fields
import_fug = concat_req_field(fug_fields)
import_stack = concat_req_field(stack_fields)
import_streamA = concat_req_field(streamA_fields)
import_streamB = concat_req_field(streamB_fields)
import_streamC = concat_req_field(streamC_fields)
import_streamD = concat_req_field(streamD_fields)
import_streamE = concat_req_field(streamE_fields)
import_streamF = concat_req_field(streamF_fields)
import_onsiteland = concat_req_field(onsiteland_fields)
import_onsiteother = concat_req_field(onsiteother_fields)
# Offsite treatment does not include basis of estimate codes
import_offsiteland = concat_req_field(offsiteland_fields)
import_offsiteother = concat_req_field(offsiteother_fields)

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
external_dir = set_dir(data_dir + '../../../')
tri_csv = external_dir + 'TRI/US_' + TRIyear + '_v' + tri_version + '/US_1_' + TRIyear + '_v' + tri_version + '.txt'

tri_release_output_fieldnames = ['FacilityID', 'CAS', 'FlowName', 'Unit', 'FlowAmount','Basis of Estimate','ReleaseType']


# Cycle through file importing by release type, the dictionary key
def import_TRI_by_release_type(d):
    tri = pd.DataFrame()
    for k, v in d.items():
        #create a data type dictionary
        dtype_dict = {'TRIFID':"str", 'CHEMICAL NAME':"str", 'CAS NUMBER':"str",'UNIT OF MEASURE':"str"}
        #The amount column will have the index of 4 - set to float
        dtype_dict[v[4]] = "float"
        #If a basis of estimate field is present, set its type to string
        if len(v) > 5:
            dtype_dict[v[5]] = "str"
        #log.info('Importing '+k+' releases')
        tri_part = pd.read_csv(tri_csv, sep='\t', header=0, usecols=v, dtype=dtype_dict,na_values=['NO'])
        #If errors are produced on import, can add param above error_bad_lines=False
        if k.startswith('offsite'):
            tri_part['Basis of Estimate'] = 'NA'
        tri_part['ReleaseType'] = k
        tri_part.columns = tri_release_output_fieldnames
        tri = pd.concat([tri,tri_part])
    return tri


tri = import_TRI_by_release_type(import_dict)
len(tri)
# 953004 for 2016
# 980196 for 2015
# 994032 for 2014
# 995712 for 2013
# 992364 for 2012
# 988848 for 2011

# drop NA for Amount, but leave in zeros
tri = tri.dropna(subset=['FlowAmount'])
len(tri)
#531157 for 2016
#546111 for 2015
#553481 for 2014
#554590 for 2013
#553041 for 2012
#551027 for 2011


# There is white space after some basis of estimate codes...remove it here
def strip_coln_white_space(df, coln):
    df[coln] = df[coln].str.strip()
    return df


tri = strip_coln_white_space(tri, 'Basis of Estimate')

#Convert to float if there are errors - be careful with this line
if tri['FlowAmount'].values.dtype != 'float64':
    tri['FlowAmount'] = pd.to_numeric(tri['FlowAmount'], errors='coerce')

#Drop 0 for FlowAmount
tri = tri[tri['FlowAmount'] != 0]
len(tri)
#100853 for 2016
#103619 for 2015
#104432 for 2014
#104643 for 2013
#105011 for 2012
#104399 for 2011

# Import reliability scores for TRI
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
tri = unit_convert(tri, 'Amount_kg', 'Unit', 'Pounds', lb_kg, 'FlowAmount')
tri = unit_convert(tri, 'Amount_kg', 'Unit', 'Grams', g_kg, 'FlowAmount')
# drop old amount and units
tri.drop('FlowAmount',axis=1,inplace=True)
tri.drop('Unit',axis=1,inplace=True)

# Rename cols to match reference format
tri.rename(columns={'Amount_kg':'FlowAmount'},inplace=True)
tri.rename(columns={'DQI Reliability Score':'ReliabilityScore'},inplace=True)

#Store totals by releasetype
#tri_totals_by_releasetype = tri.groupby('ReleaseType')['FlowAmount'].sum()
#tri_totals_by_releasetype.to_csv('tri_totals_by_releasetype_'+TRIyear+'.csv')


#Drop release type
tri.drop('ReleaseType',axis=1,inplace=True)

#Group by facility, flow and compartment to aggregate different release types
grouping_vars = ['FacilityID', 'FlowName','CAS','Compartment']
wm = lambda x: np.average(x, weights=tri.loc[x.index, "FlowAmount"])
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
tri_facility = pd.read_csv(tri_csv, sep='\t', header=0, usecols=import_facility, error_bad_lines=False)
#get unique facilities
tri_facility_unique_ids = pd.unique(tri_facility['TRIFID'])
len(tri_facility_unique_ids)
#2016: 21670
#2015: 22131

tri_facility_unique_rows  = tri_facility.drop_duplicates()
len(tri_facility_unique_rows)
#2016: 21738
#2015: 22195
#2014: 22291

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
try: retrieval_time = os.path.getctime(external_dir + 'TRI/US_' + TRIyear + '_v' + tri_version + '/US_1_' + TRIyear + '_v' + tri_version + '.zip')
except:
    try: retrieval_time = os.path.getctime(tri_csv)
    except: retrieval_time = time.time()
tri_metadata['SourceAquisitionTime'] = time.ctime(retrieval_time)
tri_metadata['SourceFileName'] = get_relpath(tri_csv)
tri_metadata['SourceURL'] = tri_url
tri_metadata['SourceVersion'] = tri_version

write_metadata('TRI', TRIyear, tri_metadata)
