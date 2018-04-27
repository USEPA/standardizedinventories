
#Fetch and process the RCRAInfo files. RCRA is in 1 large fixed width format txt file
#Main data site
#url = 'ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/biennial_report/'
#Lookup files
#'ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/lookup_files/'
#wastecodefileurl = "ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/lookup_files/lu_waste_code.txt.gz"

import pandas as pd
import StandardizedReleaseAndWasteInventories.globals as globals
from StandardizedReleaseAndWasteInventories.globals import unit_convert

#Get file columns widths
output_dir = globals.output_dir
data_dir = globals.data_dir
linewidthsdf = pd.read_csv(data_dir + 'RCRA_FlatFile_LineComponents_2015.csv')
BRwidths = linewidthsdf['Size']
BRnames = linewidthsdf['Data Element Name']

#Get columns to keep
RCRAfieldstokeepdf = pd.read_table(data_dir + 'RCRA_required_fields.txt', header=None)
RCRAfieldstokeep = list(RCRAfieldstokeepdf[0])

#Read in flat file for 2015 biennial report
datafile = "./data/br_reporting_2015.txt" #Do not add this file to the repository
#
BR_1 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep, nrows=100000)

#Read in one hundred thousand records at a time saving them in separate dataframes to avoid memory error
#this is the last of the files, for 2015 has about 60K plus rows. For other years you may have to adjust, or could made this into a function to iterate through files
BR_1 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep, nrows=100000)
BR_2 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=100000, nrows=100000)
BR_3 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=200000, nrows=100000)
BR_4 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=300000, nrows=100000)
BR_5 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=400000, nrows=100000)
BR_6 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=500000, nrows=100000)
BR_7 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=600000, nrows=100000)
BR_8 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=700000, nrows=100000)
BR_9 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=800000, nrows=100000)
BR_10 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=900000, nrows=100000)
BR_11 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1000000, nrows=100000)
BR_12 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1100000, nrows=100000)
BR_13 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1200000, nrows=100000)
BR_14 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1300000, nrows=100000)
BR_15 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1400000, nrows=100000)
BR_16 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1500000, nrows=100000)
BR_17 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1600000, nrows=100000)
BR_18 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1700000, nrows=100000)
BR_19 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1800000, nrows=100000)
BR_20 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1900000, nrows=100000)
BR_21 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=2000000, nrows=100000)

#Stack up the files
BR = pd.concat([BR_1,BR_2,BR_3,BR_4,BR_5,BR_6,BR_7,BR_8,
                    BR_9,BR_10,BR_11,BR_12,BR_13,BR_14,BR_15,
                    BR_16,BR_17,BR_18,BR_19,BR_20,BR_21], ignore_index=True)

#Validate correct import - number of states should be around 50 (includes PR and territories)
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
        print(item)
        SourceCodestoKeep.append(item)

BR = BR[BR['Source Code'].isin(SourceCodestoKeep)]

#Reassign the NAICS to a string
BR['NAICS'] = BR['Primary NAICS'].astype('str')
BR.drop('Primary NAICS', axis=1, inplace=True)

#Identify wastes from hazardous waste facilities
BR_wasterecords  = BR[BR['NAICS'].map(lambda x: x.startswith('562'))]
#562112', '562219', '562211', '56292', '562998', '56291', '562212','562119', '562111', '562213', '56221', '56299', '56211'
#Only 562211 is hazardous waste treatment

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

#Export to csv
BR.to_csv(output_dir + 'RCRAInfo_2015.csv',index=False)






