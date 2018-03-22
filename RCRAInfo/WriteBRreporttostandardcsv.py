import pandas as pd


BR2015 = pd.read_pickle('./data/BR2015_pickle')
sum(BR2015['Generation Tons']) #tons = 79954513
len(BR2015) #records = 2065955

#Filtering to remove double counting and non BR waste records
#Do not double count generation from sources that receive it only
#Check sum of tons and number of records after each filter step
#See EPA 2013. Biennial Report Analytical Methodologies: Data Selection Logic and Assumptions used to Analyze the Biennial Report. Office of Resource Conservation and Recovery

#Drop lines with source code G61
BR2015  = BR2015[BR2015['Source Code'] != 'G61']
sum(BR2015['Generation Tons']) #tons = 79688683
len(BR2015) #records = 1972255

#Only include wastes that are included in the National Biennial Report
BR2015 = BR2015[BR2015['Generator ID Included in NBR'] == 'Y']
sum(BR2015['Generation Tons']) #tons = 79442685
len(BR2015) #records = 1772004

BR2015 = BR2015[BR2015['Generator Waste Stream Included in NBR'] == 'Y']
sum(BR2015['Generation Tons']) #tons = 33646623
len(BR2015) #records = 288323

#Remove US territories
BR2015['State'].unique() #56
BR2015 = BR2015[BR2015['State'] != 'GU'] #Guam
BR2015 = BR2015[BR2015['State'] != 'PR'] #Puerto Rico
BR2015 = BR2015[BR2015['State'] != 'VI'] #Virgin Islands
BR2015 = BR2015[BR2015['State'] != 'MP'] #NORTHERN MARIANAS
BR2015 = BR2015[BR2015['State'] != 'NN'] #Navajo Nation

len(BR2015['State'].unique()) #states = 51 (all states plus DC)
sum(BR2015['Generation Tons']) #tons = 33625841
len(BR2015) #records = 286070

#Remove imported wastes, waste codes G63-G75
ImportSourceCodes = pd.read_csv('./data/RCRAImportSourceCodes.txt', header=None)
ImportSourceCodes = ImportSourceCodes[0].tolist()
SourceCodesPresent = BR2015['Source Code'].unique().tolist()

SourceCodestoKeep = []
for item in SourceCodesPresent:
    if item not in ImportSourceCodes:
        print(item)
        SourceCodestoKeep.append(item)

BR2015 = BR2015[BR2015['Source Code'].isin(SourceCodestoKeep)]
sum(BR2015['Generation Tons']) #tons = 33606838
len(BR2015) #records = 283903

#Reassign the NAICS to a string
BR2015.dtypes
BR2015['NAICS'] = BR2015['Primary NAICS'].astype('str')
BR2015.drop('Primary NAICS', axis=1, inplace=True)
BR2015['NAICS'].unique()

#Identify wastes from hazardous waste facilities
BR2015_wasterecords  = BR2015[BR2015['NAICS'].map(lambda x: x.startswith('562'))]
BR2015_wasterecords['NAICS'].unique()
#562112', '562219', '562211', '56292', '562998', '56291', '562212','562119', '562111', '562213', '56221', '56299', '56211'
#Only 562211 is hazardous waste treatment



#Create field for DQI Reliability Score with fixed value from CSV
#Currently generating a warning
reliabilitytable = pd.read_csv('./data/DQ_Reliability_Scores_Table3-3fromERGreport.csv', usecols=['Source','DQI Reliability Score'])
rcrainfo_reliabilitytable = reliabilitytable[reliabilitytable['Source']=='RCRAInfo']
rcrainfo_reliabilitytable.drop('Source', axis=1, inplace=True)
print(rcrainfo_reliabilitytable)
BR2015['ReliabilityScore'] = float(rcrainfo_reliabilitytable['DQI Reliability Score'])

#Create a new field to put converted amount in
BR2015['Amount_kg'] = 0.0
#Convert amounts from tons. Note this could be replaced with a conversion utility
BR2015['Amount_kg'] = 907.18474*BR2015['Generation Tons']

#Drop unneeded fields
BR2015.drop('Generation Tons', axis=1, inplace=True)
BR2015.drop('Generator ID Included in NBR', axis=1, inplace=True)
BR2015.drop('Generator Waste Stream Included in NBR', axis=1, inplace=True)
BR2015.drop('Source Code', axis=1, inplace=True)
BR2015.drop('Management Method', axis=1, inplace=True)
BR2015.drop('Waste Code Group', axis=1, inplace=True)
BR2015.drop('Waste Description', axis=1, inplace=True)

BR2015.rename(columns={'Amount_kg':'Amount'}, inplace=True)
BR2015.rename(columns={'Form Code':'OriginalFlowID'}, inplace=True)
BR2015.rename(columns={'Handler ID':'FacilityID'}, inplace=True)

#Reorder columns to standard format
BR2015 = BR2015.reindex(columns=['OriginalFlowID', 'Amount', 'State', 'NAICS', 'FacilityID', 'ReliabilityScore','Federal Waste Flag'])

#Export to csv
BR2015.to_csv('./output/RCRAInfo_2015.csv',index=False)

