
#Fetch and process the RCRAInfo files. RCRA is in 1 large fixed width format txt file
#Main data site
#url = 'ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/biennial_report/'
#Lookup files
#'ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/lookup_files/'
#wastecodefileurl = "ftp://ftp.epa.gov/rcrainfodata/rcra_flatfiles/lookup_files/lu_waste_code.txt.gz"

import pandas as pd

#Set output directory
outputdir = ''

#Get file columns widths
linewidthsdf = pd.read_csv('../data/RCRA_FlatFile_LineComponents_2015.csv')
linewidthsdf.columns
BRwidths = linewidthsdf['Size']
BRnames = linewidthsdf['Data Element Name']
BRnames

#Get columns to keep
RCRAfieldstokeepdf = pd.read_table('../data/RCRA_required_fields.txt', header=None)
RCRAfieldstokeep = list(RCRAfieldstokeepdf[0])
RCRAfieldstokeep 

#Read in flat file for 2015 biennial report
datafile = "../data/br_reporting_2015.txt" #Do not add this file to the repository
#Read in one hundred thousand records at a time saving them in separate dataframes to avoid memory error
#this is the last of the files, for 2015 has about 60K plus rows. For other years you may have to adjust, or could made this into a function to iterate through files
BR2015_1 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep, nrows=100000)
BR2015_2 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=100000, nrows=100000)
BR2015_3 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=200000, nrows=100000)
BR2015_4 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=300000, nrows=100000)
BR2015_5 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=400000, nrows=100000)
BR2015_6 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=500000, nrows=100000)
BR2015_7 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=600000, nrows=100000)
BR2015_8 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=700000, nrows=100000)
BR2015_9 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=800000, nrows=100000)
BR2015_10 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=900000, nrows=100000)
BR2015_11 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1000000, nrows=100000)
BR2015_12 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1100000, nrows=100000)
BR2015_13 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1200000, nrows=100000)
BR2015_14 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1300000, nrows=100000)
BR2015_15 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1400000, nrows=100000)
BR2015_16 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1500000, nrows=100000)
BR2015_17 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1600000, nrows=100000)
BR2015_18 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1700000, nrows=100000)
BR2015_19 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1800000, nrows=100000)
BR2015_20 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=1900000, nrows=100000)
BR2015_21 = pd.read_fwf(datafile,widths=BRwidths,header=None,names=BRnames,usecols=RCRAfieldstokeep,skiprows=2000000, nrows=100000)

#Stack up the files
BR2015 = pd.concat([BR2015_1,BR2015_2,BR2015_3,BR2015_4,BR2015_5,BR2015_6,BR2015_7,BR2015_8,
                    BR2015_9,BR2015_10,BR2015_11,BR2015_12,BR2015_13,BR2015_14,BR2015_15,
                    BR2015_16,BR2015_17,BR2015_18,BR2015_19,BR2015_20,BR2015_21], ignore_index=True)

#Validate correct import
#check state names
#states = BR2015['State'].unique()
states
len(states)

#Check sum of generation, should compute without error
sum(BR2015['Generation Tons'])

#Save as Python dataframe
BR2015.to_pickle('../data/BR2015_pickle')
#