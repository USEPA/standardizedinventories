import pandas as pd


BR2015 = pd.read_pickle('../data/BR2015_pickle')

#Create field for DQI Reliability Score with fixed value from CSV
#Currently generating a warning
reliabilitytable = pd.read_csv('../data/DQ_Reliability_Scores_Table3-3fromERGreport.csv', usecols=['Source','DQI Reliability Score'])
rcrainfo_reliabilitytable = reliabilitytable[reliabilitytable['Source']=='RCRAInfo']
rcrainfo_reliabilitytable.drop('Source', axis=1, inplace=True)
print(rcrainfo_reliabilitytable)
BR2015['DQI Reliability Score'] = float(rcrainfo_reliabilitytable['DQI Reliability Score'])

#Create a new field to put converted amount in
BR2015['Amount_kg'] = 0.0
#Convert amounts from tons. Note this could be replaced with a conversion utility
BR2015['Amount_kg'] = 907.18474*BR2015['Generation Tons']
BR2015.drop('Generation Tons', axis=1, inplace=True)
BR2015.rename(columns={'Amount_kg':'Amount'}, inplace=True)
BR2015.rename(columns={'DQI Reliability Score':'ReliabilityScore'}, inplace=True)
BR2015.rename(columns={'Form Code':'OriginalFlowID'}, inplace=True)
BR2015.rename(columns={'Handler ID':'FacilityID'}, inplace=True)
BR2015.rename(columns={'Primary NAICS':'NAICS'}, inplace=True)

#Reorder columns to standard format
BR2015 = BR2015.reindex(columns=['OriginalFlowID', 'Amount', 'State', 'NAICS', 'FacilityID', 'ReliabilityScore'])

#Export to csv
BR2015.to_csv('../output/RCRAInfo_2015.csv',index=False)

