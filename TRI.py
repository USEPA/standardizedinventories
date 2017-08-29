#TRI import and processing
#This script uses the TRI Basic National Data File.
#Data files:https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-data-files-calendar-years-1987-2016
#2015 documentation on file format: https://www.epa.gov/sites/production/files/2016-11/documents/tri_basic_data_file_format_v15.pdf
#The format may change from yr to yr requiring code updates. 
#This code has been tested for 2014.

import pandas as pd

#Set some metadata
TRIyear = '2015'

#Do not write intermediate and final output to the code folder. Specify the output directory here
outputdir = '../LCI-Primer-Output/'

#Import list of fields from TRI that are desired for LCI
tri_required_fields_csv = 'data/TRI_required_fields.txt'
tri_required_fields =  pd.read_table(tri_required_fields_csv, header=None)
tri_required_fields = list(tri_required_fields[0])
tri_required_fields

#Import in pieces grabbing main fields plus unique amount and basis of estimate fields
import_fug =  tri_required_fields[0:5]+ tri_required_fields[5:7]
import_stack =  tri_required_fields[0:5]+ tri_required_fields[7:9]
import_streamA =  tri_required_fields[0:5]+ tri_required_fields[9:11]
import_streamB =  tri_required_fields[0:5]+ tri_required_fields[11:13]
import_streamC =  tri_required_fields[0:5]+ tri_required_fields[13:15]
import_streamD =  tri_required_fields[0:5]+ tri_required_fields[15:17]
import_streamE =  tri_required_fields[0:5]+ tri_required_fields[17:19]
import_streamF =  tri_required_fields[0:5]+ tri_required_fields[19:21]
import_onsiteland =  tri_required_fields[0:5]+ tri_required_fields[21:23]
import_onsiteother =  tri_required_fields[0:5]+ tri_required_fields[23:25]
#Offsite treatment does not include basis of estimate codes
import_offsiteland =  tri_required_fields[0:5]+ tri_required_fields[25:26]
import_offsiteother =  tri_required_fields[0:5]+ tri_required_fields[26:27]

#Examine each to make sure they are right
#import_fug
#import_stack
#import_streamA
#import_streamB
#import_streamC
#import_streamD
#import_streamE
#import_streamF
#import_onsiteland
#import_onsiteother
#import_offsiteland
#import_offsiteother

#Create a dictionary that had the import fields for each release type to use in import process
import_dict = {'fug' : import_fug, 
'stack': import_stack,
'streamA' : import_streamA,
'streamB': import_streamB,
'streamC': import_streamC,
'streamD': import_streamD,
'streamE': import_streamE,
'streamF': import_streamF,
'onsiteland': import_onsiteland,
'onsiteother': import_onsiteother,
'offsiteland': import_offsiteland,
'offsiteother': import_offsiteother}


#Import TRI file
tri_csv = "TRI_example.csv" #Use example for testing
#tri_csv = "....TRI_2015_US.csv" #TRI file .. do not include in repository folder
tri = pd.DataFrame()
fieldnames = ['FacilityID','State', 'NAICS', 'OriginalFlowID','Unit','Amount','Basis of Estimate']
l = len(fieldnames)-1
fieldnamesshort = fieldnames[0:l]
#import_dict
for k,v in import_dict.items():
    tri_part = pd.read_csv(tri_csv, header=0,usecols=v,error_bad_lines=False)
    if k.startswith('offsite'):
        tri_part.columns = fieldnamesshort
    else:
        tri_part.columns = fieldnames
    #drop NA for Amount, but leave in zeros    
    tri_part = tri_part.dropna(subset = ['Amount'])
    #Set 'Source' to key for name of datatype
    tri_part['Source'] = k
    #Strip white space from basis of estimate
    tri = pd.concat([tri, tri_part])

#There is white space after some basis of estimate codes...remove it here
tri['Basis of Estimate'] = tri['Basis of Estimate'].str.strip()
#Show first 50 to see
tri.head(50)
#Export for review
tri.to_csv(outputdir + '1_trifirststep.csv')

#Import reliability scores for TRI
reliabilitytable = pd.read_csv('data/DQ_Reliability_Scores_Table3-3fromERGreport.csv', usecols=['Source','Code','DQI Reliability Score'])
trireliabilitytable = reliabilitytable[reliabilitytable['Source']=='TRI']
trireliabilitytable.drop('Source', axis=1, inplace=True)
trireliabilitytable

#Link Basis of Estimate to Data Reliability Scores
tri2 = pd.merge(tri,trireliabilitytable, left_on='Basis of Estimate', right_on='Code', how='left')
#Fill NAs with 5 for DQI reliability score
tri2['DQI Reliability Score'] = tri2['DQI Reliability Score'].fillna(value=5)
#Drop unneeded columns
tri2.drop('Basis of Estimate', axis=1, inplace=True)
tri2.drop('Code', axis=1, inplace=True)
#Export for review
tri2.to_csv(outputdir + '2_triafterreliabilitymerge.csv')

#Replace source info with Context
source_to_context = pd.read_csv('TRI_Source_to_Context.csv')
source_to_context
tri3 = pd.merge(tri2,source_to_context)
tri3.head(100)

#Import pollutant omit list and use it to remove the pollutants to omit
omitlist = pd.read_csv('TRI_pollutant_omit_list.csv')
omitIDs = omitlist['OriginalFlowID']
omitIDs

#!Still need to implement code for this removal. 

#Convert units to ref mass unit of kg
tri = tri3
#Create a new field to put converted amount in
tri['Amount_kg'] = 0.0
#Convert amounts. Note this could be replaced with a conversion utility
tri['Amount_kg'][tri['Unit'] == 'Pounds'] = 0.45392*tri['Amount']
tri['Amount_kg'][tri['Unit'] == 'Grams'] = 0.001*tri['Amount']
tri.to_csv(outputdir + '3_triwithamountsconvertedshowingoldunits.csv')

#drop old amount and units
tri.drop('Amount', axis=1, inplace=True)
tri.drop('Unit', axis=1, inplace=True)

#Final cleanup - first rename then reorder
tri.rename(columns={'Amount_kg':'Amount'}, inplace=True)
tri.rename(columns={'DQI Reliability Score':'ReliabilityScore'}, inplace=True)

#See final names and ordering from reference list
reflist = pd.read_csv('Standarized_Output_Format_EPA _Data_Sources.csv')
reflist = reflist[reflist['required?']==1]
refnames = list(reflist['Name'])
refnames.append('Source')
refnames
tri.to_csv(outputdir + '4_triwithamountsconverted.csv')


#Reorder columns to match standard format
tri = tri.reindex(columns=refnames)

#!Save as R dataframe. Still tbd
#Use rpy2  package

#Export it as a csv
#Final file name
tri_file_name = 'TRI_'+ TRIyear + '_standard_format.csv'
tri.to_csv(outputdir + tri_file_name)
