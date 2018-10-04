#Fetch and process the RCRAInfo Biennial Report files. Biennial Report is in 1 large fixed width format txt file
#Main data file
#ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/Baseline/biennial_report.zip

#Lookup files
#ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/Baseline/lookup_files.zip

import pandas as pd
import stewi.globals as globals
from stewi.globals import write_metadata,unit_convert,validate_inventory,write_validation_result,set_dir,output_dir,data_dir
import gzip
import shutil
import urllib
import numpy as np

#Valid years are every other year since 2015
report_year = '2015'

#Get file columns widths
linewidthsdf = pd.read_csv(data_dir + 'RCRA_FlatFile_LineComponents.csv')
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
RCRAInfopath = set_dir(data_dir + "../../../RCRAInfo/")
RCRAInfoBRzip = RCRAInfopath + 'biennial_report.zip'
RCRAInfoBRarchivefile = RCRAInfopath + 'br_reporting_' + report_year + '.txt.gz'
RCRAInfoBRtextfile = RCRAInfopath + 'br_reporting_' + report_year + '.txt'

retrieval_time = None


def extract_gzip(input_path, output_path):
    with gzip.open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


if checkforFile(RCRAInfoBRtextfile) is False:
    while checkforFile(RCRAInfoBRarchivefile) is False:
        RCRAfile = urllib.request.urlretrieve(RCRAfInfoflatfileURL, RCRAInfoBRzip)

        #download file
        #get timestamp
        import time
        retrieval_time = time.time()
        break

        #unzip it
    extract_gzip(RCRAInfoBRarchivefile, RCRAInfoBRtextfile)

#Get total tow count of the file
with open(RCRAInfoBRtextfile, 'rb') as rcrafile:
    row_count = sum(1 for row in rcrafile)


#Test 10 records
BRtest = pd.read_fwf(RCRAInfoBRtextfile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep, nrows=10)

#Read in one hundred thousand records at a time
BR = pd.DataFrame()
for chunk in pd.read_fwf(RCRAInfoBRtextfile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,chunksize=100000):
    BR = pd.concat([BR,chunk])

#Pickle as a backup
#BR.to_pickle('work/BR_'+report_year+'.pk')
#Read in to start from a pickle
#BR = pd.read_pickle('work/BR_'+report_year+'.pk')
len(BR)
#2015:2053108
#2013:1581899
#2011: 1590067

#Validate correct import - number of states should be 50+ (includes PR and territories)
states = BR['State'].unique()
len(states)
#2015: 57
#2015: 56
#2011: 56

#Filtering to remove double counting and non BR waste records
#Do not double count generation from sources that receive it only
#Check sum of tons and number of records after each filter step
#See EPA 2013. Biennial Report Analytical Methodologies: Data Selection Logic and Assumptions used to Analyze the Biennial Report. Office of Resource Conservation and Recovery

#Drop lines with source code G61
BR = BR[BR['Source Code'] != 'G61']
len(BR)
#2015:1959883
#2013:1492245
#2011:1496275

#Only include wastes that are included in the National Biennial Report
BR = BR[BR['Generator ID Included in NBR'] == 'Y']
len(BR)
#2015:1759711
#2013:1283457
#2011:1284796

BR = BR[BR['Generator Waste Stream Included in NBR'] == 'Y']
len(BR)
#2015:288980
#2013:256978
#2011:209342

#Remove imported wastes, source codes G63-G75
ImportSourceCodes = pd.read_csv(data_dir + 'RCRAImportSourceCodes.txt', header=None)
ImportSourceCodes = ImportSourceCodes[0].tolist()
SourceCodesPresent = BR['Source Code'].unique().tolist()

SourceCodestoKeep = []
for item in SourceCodesPresent:
    if item not in ImportSourceCodes:
        #print(item)
        SourceCodestoKeep.append(item)

BR = BR[BR['Source Code'].isin(SourceCodestoKeep)]
len(BR)
#2015:286813
#2013:256844
#2011:209306

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
from stewi.globals import USton_kg
BR['Amount_kg'] = USton_kg*BR['Generation Tons']

##Read in waste descriptions
linewidthsdf = pd.read_csv(data_dir + 'RCRAInfo_LU_WasteCode_LineComponents.csv')
widths = linewidthsdf['Size']
names = linewidthsdf['Data Element Name']
wastecodesfile = RCRAInfopath + 'lu_waste_code.txt'
if not checkforFile(wastecodesfile):
    if checkforFile(wastecodesfile + '.gz'): extract_gzip(wastecodesfile + '.gz', wastecodesfile)
WasteCodesTest = pd.read_fwf(wastecodesfile,widths=widths,header=None,names=names,nrows=10)
WasteCodes = pd.read_fwf(wastecodesfile,widths=widths,header=None,names=names)
WasteCodes = WasteCodes[['Waste Code', 'Code Type', 'Waste Code Description']]
#Remove rows where any fields are na description is missing
WasteCodes.dropna(inplace=True)

#Bring in form codes
#Replace form code with the code name
form_code_name_file = data_dir + 'RCRA_LU_FORM_CODE.csv'
form_code_table_cols_needed = ['FORM_CODE','FORM_CODE_NAME']
form_code_name_df = pd.read_csv(form_code_name_file,header=0,usecols=form_code_table_cols_needed)

#Merge waste codes with BR records
BR = pd.merge(BR,WasteCodes,left_on='Waste Code Group',right_on='Waste Code',how='left')
#Rename code type to make it clear
BR.rename(columns={'Code Type':'Waste Code Type'},inplace=True)
#Merge form codes with BR
BR = pd.merge(BR,form_code_name_df,left_on='Form Code',right_on='FORM_CODE',how='left')
#Drop duplicates from merge
BR.drop(columns=['FORM_CODE','Waste Code Group'], inplace=True)

#Set flow name to Waste Code Description
BR['FlowName'] = BR['Waste Code Description']
#BR['FlowNameSource'] = 'Waste Code Description'
#If a useful Waste Code Description is present, use it
def waste_description_cleaner(x):
    if (x == 'from br conversion') or (x =='From 1989 BR data'):
        x = None
    return x
BR['FlowName'] = BR['FlowName'].apply(waste_description_cleaner)
#Check unique flow names
pd.unique(BR['FlowName'])

#If there is not useful waste code, fill it with the Form Code Name
#Find the NAs in FlowName and then give that source of Form Code
BR.loc[BR['FlowName'].isnull(),'FlowNameSource'] = 'Form Code'
#Now for those source name rows that are blank, tell it its a waste code
BR.loc[BR['FlowNameSource'].isnull(),'FlowNameSource'] = 'Waste Code'

#Set FlowIDs to the appropriate code
BR.loc[BR['FlowName'].isnull(),'FlowID'] = BR['Form Code']
BR.loc[BR['FlowID'].isnull(),'FlowID'] = BR['Waste Code']

#Now finally fill names that are blank with the form code name
BR['FlowName'].fillna(BR['FORM_CODE_NAME'], inplace=True)

#Drop unneeded fields
BR.drop('Generation Tons', axis=1, inplace=True)
BR.drop('Generator ID Included in NBR', axis=1, inplace=True)
BR.drop('Generator Waste Stream Included in NBR', axis=1, inplace=True)
BR.drop('Source Code', axis=1, inplace=True)
BR.drop('Management Method', axis=1, inplace=True)
BR.drop('Waste Description', axis=1, inplace=True)
BR.drop('Waste Code Description', axis=1, inplace=True)
BR.drop('FORM_CODE_NAME', axis=1, inplace=True)

#Rename cols used by multiple tables
BR.rename(columns={'Handler ID':'FacilityID'}, inplace=True)
#rename new name
BR.rename(columns={'Amount_kg':'FlowAmount'}, inplace=True)

#Prepare flows file
flows = BR[['FlowName','FlowID','FlowNameSource']]
#Drop duplicates
flows = flows.drop_duplicates()
flows['Compartment']='Waste'
flows['Unit']='kg'
#Sort them by the flow names
flows.sort_values(by='FlowName',axis=0,inplace=True)
#Export them
flows.to_csv(output_dir + 'flow/RCRAInfo_' + report_year + '.csv',index=False)

#Prepare facilities file
facilities = BR[['FacilityID', 'Handler Name','Location Street Number',
       'Location Street 1', 'Location Street 2', 'Location City',
       'Location State', 'Location Zip', 'County Name','NAICS']]
#Drop duplicates
facilities.drop_duplicates(inplace=True)

facilities['Location Street Number'] = facilities['Location Street Number'].apply(str)
facilities['Location Street Number'].fillna('',inplace=True)

facilities['Address'] = facilities['Location Street Number'] + ' ' + facilities['Location Street 1'] + ' ' + facilities['Location Street 2']
facilities.drop(columns=['Location Street Number','Location Street 1','Location Street 2'],inplace=True)

facilities.rename(columns={'Primary NAICS':'NAICS',
                           'Handler Name':'FacilityName',
                           'Location City':'City',
                           'Location State':'State',
                           'Location Zip':'Zip',
                           'County Name':'County'}, inplace=True)
facilities.to_csv(output_dir + 'facility/RCRAInfo_' + report_year + '.csv',index=False)

#Prepare flow by facility

flowbyfacility = BR.groupby(['FacilityID','ReliabilityScore','FlowName'])['FlowAmount'].sum().reset_index()

##VALIDATION
BR_national_total = pd.read_csv(data_dir + 'RCRAInfo_' + report_year + '_NationalTotals.csv', header=0, dtype={"FlowAmount":np.float})
BR_national_total['FlowAmount_kg'] = 0
BR_national_total = unit_convert(BR_national_total, 'FlowAmount_kg', 'Unit', 'Tons', 907.18474, 'FlowAmount')
BR_national_total.drop('FlowAmount',axis=1,inplace=True)
BR_national_total.drop('Unit',axis=1,inplace=True)
# Rename cols to match reference format
BR_national_total.rename(columns={'FlowAmount_kg':'FlowAmount'},inplace=True)

#Validate total waste generated against national totals
sum_of_flowbyfacility = flowbyfacility['FlowAmount'].sum()
sum_of_flowbyfacility_df = pd.DataFrame({'FlowAmount':[sum_of_flowbyfacility],'FlowName':'ALL','Compartment':'waste'})
validation_df = validate_inventory(sum_of_flowbyfacility_df,BR_national_total,group_by='flow')
write_validation_result('RCRAInfo', report_year, validation_df)

#Export to csv
flowbyfacility.to_csv(output_dir + 'flowbyfacility/RCRAInfo_' + report_year + '.csv',index=False)

#Record metadata
if retrieval_time is not None:
    BR_meta['SourceAquisitionTime'] = time.ctime(retrieval_time)
BR_meta['SourceFileName'] = RCRAInfoBRtextfile
BR_meta['SourceURL'] = RCRAfInfoflatfileURL

write_metadata('RCRAInfo',report_year, BR_meta)



