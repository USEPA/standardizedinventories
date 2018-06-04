#Fetch and process the RCRAInfo Biennial Report files. Biennial Report is in 1 large fixed width format txt file
#Main data file
#ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/Baseline/biennial_report.zip

#Lookup files
#ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/Baseline/lookup_files.zip

import pandas as pd
import stewi.globals as globals
from stewi.globals import write_metadata
import os
import gzip
import shutil
import urllib

#Valid years are every other year since 2015
report_year = '2013'

#Get file columns widths
output_dir = globals.output_dir
data_dir = globals.data_dir
linewidthsdf = pd.read_csv(data_dir + 'RCRA_FlatFile_LineComponents_2015.csv')
BRwidths = linewidthsdf['Size']
BRnames = linewidthsdf['Data Element Name']

#Metadata
BR_meta = globals.inventory_metadata

#Get columns to keep
RCRAfieldstokeepdf = pd.read_table(data_dir + 'RCRA_required_fields.txt', header=None)
RCRAfieldstokeep = list(RCRAfieldstokeepdf[0])

#Read in flat file for 2015 biennial report
from stewi.globals import checkforFile

RCRAfInfoflatfileURL = 'ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/Baseline/biennial_report.zip'
RCRAInfopath = "../RCRAInfo/"
RCRAInfoBRzip = RCRAInfopath+'biennial_report.zip'
RCRAInfoBRarchivefile = RCRAInfopath+'br_reporting_' + report_year + '.txt.gz'
RCRAInfoBRtextfile =  RCRAInfopath+'br_reporting_' + report_year + '.txt'

retrieval_time = None

if checkforFile(RCRAInfoBRtextfile) is False:
    while checkforFile(RCRAInfoBRarchivefile) is False:
        RCRAfile = urllib.request.urlretrieve(RCRAfInfoflatfileURL, RCRAInfoBRzip)

        #download file
        #get timestamp
        import time
        retrieval_time = time.time()
        break

        #unzip it
    with gzip.open(RCRAInfoBRarchivefile, 'rb') as f_in:
        with open(RCRAInfoBRtextfile, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

#Get total tow count of the file
with open(RCRAInfoBRtextfile, 'rb') as rcrafile:
    row_count = sum(1 for row in rcrafile)


#Test 10 records
BRtest = pd.read_fwf(RCRAInfoBRtextfile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep, nrows=10)


#Read in one hundred thousand records at a time saving them in separate dataframes to avoid memory error
BR = pd.DataFrame()
for chunk in pd.read_fwf(RCRAInfoBRtextfile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,chunksize=100000):
    BR = pd.concat([BR,chunk])

#Pickle as a backup
#BR.to_pickle('BR_'+report_year+'.pk')

#Validate correct import - number of states should be 50+ (includes PR and territories)
states = BR['State'].unique()

#Filtering to remove double counting and non BR waste records
#Do not double count generation from sources that receive it only
#Check sum of tons and number of records after each filter step
#See EPA 2013. Biennial Report Analytical Methodologies: Data Selection Logic and Assumptions used to Analyze the Biennial Report. Office of Resource Conservation and Recovery

#Drop lines with source code G61
BR = BR[BR['Source Code'] != 'G61']

#Only include wastes that are included in the National Biennial Report
BR = BR[BR['Generator ID Included in NBR'] == 'Y']

BR = BR[BR['Generator Waste Stream Included in NBR'] == 'Y']

#Remove imported wastes, waste codes G63-G75
ImportSourceCodes = pd.read_csv(data_dir + 'RCRAImportSourceCodes.txt', header=None)
ImportSourceCodes = ImportSourceCodes[0].tolist()
SourceCodesPresent = BR['Source Code'].unique().tolist()

SourceCodestoKeep = []
for item in SourceCodesPresent:
    if item not in ImportSourceCodes:
        #print(item)
        SourceCodestoKeep.append(item)

BR = BR[BR['Source Code'].isin(SourceCodestoKeep)]

#Reassign the NAICS to a string
BR['NAICS'] = BR['Primary NAICS'].astype('str')
BR.drop('Primary NAICS', axis=1, inplace=True)

#Create field for DQI Reliability Score with fixed value from CSV
#Currently generating a warning
reliability_table = globals.reliability_table
rcrainfo_reliability_table = reliability_table[reliability_table['Source']=='RCRAInfo']
rcrainfo_reliability_table.drop('Source', axis=1, inplace=True)
BR['ReliabilityScore'] = float(rcrainfo_reliability_table['DQI Reliability Score'])

#Create a new field to put converted amount in
BR['Amount_kg'] = 0.0
#Convert amounts from tons. Note this could be replaced with a conversion utility
BR['Amount_kg'] = 907.18474*BR['Generation Tons']

#Drop unneeded fields
BR.drop('Generation Tons', axis=1, inplace=True)
BR.drop('Generator ID Included in NBR', axis=1, inplace=True)
BR.drop('Generator Waste Stream Included in NBR', axis=1, inplace=True)
BR.drop('Source Code', axis=1, inplace=True)
BR.drop('Management Method', axis=1, inplace=True)
BR.drop('Waste Code Group', axis=1, inplace=True)
BR.drop('Waste Description', axis=1, inplace=True)

#Replace form code with the code name
form_code_name_file = data_dir + 'RCRA_LU_FORM_CODE.csv'
form_code_table_cols_needed = ['FORM_CODE','FORM_CODE_NAME']
form_code_name_df = pd.read_csv(form_code_name_file,header=0,usecols=form_code_table_cols_needed)
#Merge with BR
BR = pd.merge(BR,form_code_name_df,left_on='Form Code',right_on='FORM_CODE',how='left')
#drop codes no longer needed
BR.drop('Form Code', axis=1, inplace=True)
BR.drop('FORM_CODE', axis=1, inplace=True)

#rename new name; drop codes and replace with names
BR.rename(columns={'FORM_CODE_NAME':'FlowName'}, inplace=True)
BR.rename(columns={'Amount_kg':'FlowAmount'}, inplace=True)
BR.rename(columns={'Handler ID':'FacilityID'}, inplace=True)
BR.rename(columns={'Primary NAICS':'NAICS'}, inplace=True)

#Export to csv
BR.to_csv(output_dir + 'RCRAInfo_' + report_year + '.csv',index=False)

#Record metadata
if retrieval_time is not None:
    BR_meta['SourceAquisitionTime'] = time.ctime(retrieval_time)
BR_meta['SourceFileName'] = RCRAInfoBRtextfile
BR_meta['SourceURL'] = RCRAfInfoflatfileURL

write_metadata('RCRAInfo',report_year, BR_meta)



