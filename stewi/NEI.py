#NEI import and process to Standardized EPA output format
#This script uses the NEI data exports from EIS.

from stewi.globals import set_dir,output_dir,data_dir, write_metadata,inventory_metadata,get_relpath,unit_convert,\
    validate_inventory,write_validation_result,USton_kg,lb_kg
import pandas as pd
import numpy as np
import os
import time

report_year = '2017'

external_dir = set_dir('../NEI/')

nei_required_fields = pd.read_table(data_dir + 'NEI_required_fields_2017.csv',sep=',').fillna('Null')
nei_file_path = pd.read_table(data_dir + 'NEI_' + report_year + '_file_path.csv',sep=',').fillna('Null')

def read_data(source,file):
    #tmp = pd.Series(list(nei_required_fields[source]), index=list(nei_required_fields['StandardizedEPA']))
    file_result = pd.DataFrame(columns=list(nei_required_fields['StandardizedEPA']))
    # read nei file by chunks
    for file_chunk in pd.read_table(external_dir + file,sep=',',usecols=list(set(nei_required_fields[source])-set(['Null'])),chunksize=100000,engine='python'):
        # change column names to Standardized EPA names
        file_chunk = file_chunk.rename(columns=pd.Series(list(nei_required_fields['StandardizedEPA']),index=list(nei_required_fields[source])).to_dict())
        # adjust column order
        file_chunk = file_chunk[file_result.columns.tolist()]
        # concatenate all chunks
        file_result = pd.concat([file_result,file_chunk])
    return(file_result)


def standardize_output(source): # source as 'Point'/'NonPoint'/'OnRoad'/'NonRoad'
    # extract file paths
    file_path = list(set(nei_file_path[source]) - set(['Null']))
    print(file_path)
    # read in first nei file by chunks
    nei = read_data(source, file_path[0])
    print(file_path[0])
    print(len(nei))
    # read in other nei files and concatenate all nei files into one dataframe
    for file in file_path[1:]:
        # concatenate all other files
        nei = pd.concat([nei,read_data(source,file)])
        print(file)
        print(len(nei))
    # convert TON to KG
    nei['FlowAmount'] = nei['FlowAmount']*USton_kg
    # add Reliability Score
    if source == 'Point':
        reliability_table = pd.read_csv(data_dir + 'DQ_Reliability_Scores_Table3-3fromERGreport.csv',usecols=['Source','Code','DQI Reliability Score'])
        nei_reliability_table = reliability_table[reliability_table['Source'] == 'NEI']
        nei_reliability_table['Code'] = nei_reliability_table['Code'].astype(float)
        nei['ReliabilityScore'] = nei['ReliabilityScore'].astype(float)
        nei = nei.merge(nei_reliability_table, left_on='ReliabilityScore', right_on='Code', how='left')
        nei['ReliabilityScore'] = nei['DQI Reliability Score']
        # drop Code and DQI Reliability Score columns
        nei = nei.drop(['Code', 'DQI Reliability Score'], 1)
    else:
        nei['ReliabilityScore'] = 3
    # add Source column
    nei['Source'] = source
    return(nei)


def nei_aggregate_unit_to_facility_level(nei_):
    # drops rows if flow amount or reliability score is zero
    nei_ = nei_[(nei_['FlowAmount'] > 0) & (nei_['ReliabilityScore'] > 0)]

    grouping_vars = ['FacilityID', 'FlowName']

    # Do groupby with sum of flow amount and weighted avg of reliabilty
    # Define a lambda function to compute the weighted avg
    wm = lambda x: np.average(x, weights=nei_.loc[x.index, "FlowAmount"])
    # Groupby and aggregate:
    neibyfacility = nei_.groupby(grouping_vars).agg({'FlowAmount': ['sum'], 'ReliabilityScore': [wm]})

    neibyfacility = neibyfacility.reset_index()
    neibyfacility.columns = neibyfacility.columns.droplevel(level=1)

    return(neibyfacility)


# Computes pollutant national totals from 'Facility-level by Pollutant' data downloaded from EPA website 
def generate_national_totals(year):
    df = pd.read_csv(data_dir + 'NEI_Facility-level by Pollutant_' + year + '.csv', header = 0,
                     usecols = ['pollutant code','pollutant desc','total emissions','emissions uom'])
    df.columns = ['FlowID', 'FlowName', 'FlowAmount', 'UOM']
    # convert LB/TON to KG
    df['FlowAmount'] = np.where(df['UOM']=='LB',df['FlowAmount']*lb_kg,df['FlowAmount']*USton_kg)
    df = df.drop(['UOM'],1)
    df = df.groupby(['FlowID','FlowName'])['FlowAmount'].sum().reset_index()
    #df = df.groupby('FlowID','FlowName').agg({'FlowAmount': ['sum']})
    df.rename(columns={'FlowAmount':'FlowAmount[kg]'}, inplace=True)
    df.to_csv(data_dir+'NEI_'+year+'_NationalTotals.csv',index=False)


#NEIPoint
nei_point = standardize_output('Point')

#Pickle it
nei_point.to_pickle('work/NEI_' + report_year + '.pk')

#Flowbyfacility output
#re_index nei_point
nei_point = nei_point.reset_index()
nei_flowbyfacility = nei_aggregate_unit_to_facility_level(nei_point)
#nei_flowbyfacility.to_csv(output_dir+'flowbyfacility/NEI_'+report_year+'.csv',index=False)
nei_flowbyfacility.to_parquet(output_dir+'flowbyfacility/NEI_'+report_year+'.parquet',
                              index=False, compression=None)
print(len(nei_flowbyfacility))
#2017: 2184786
#2016: 1965918
#2014: 2057249
#2011: 1840866

##Flows output
nei_flows = nei_point[['FlowName', 'FlowID']]
nei_flows = nei_flows.drop_duplicates()
nei_flows['Compartment']='air'
nei_flows['Unit']='kg'
nei_flows = nei_flows.sort_values(by='FlowName',axis=0)
nei_flows.to_csv(output_dir+'flow/'+'NEI_'+report_year+'.csv',index=False)
print(len(nei_flows))
#2017: 293
#2016: 282
#2014: 279
#2011: 277

##Facility output
facility = nei_point[['FacilityID', 'FacilityName', 'Address', 'City', 'State',
       'Zip', 'Latitude', 'Longitude', 'NAICS', 'County']]
facility = facility.drop_duplicates('FacilityID')
facility.to_csv(output_dir+'facility/'+'NEI_'+report_year+'.csv',index=False)
print(len(facility))
#2017: 87162
#2016: 85802
#2014: 85125
#2011: 95565

#Write metadata
NEI_meta = inventory_metadata

#Get time info from first point file
point_1_path = external_dir + nei_file_path['Point'][0]
nei_retrieval_time = time.ctime(os.path.getctime(point_1_path))

if nei_retrieval_time is not None:
    NEI_meta['SourceAquisitionTime'] = nei_retrieval_time
NEI_meta['SourceFileName'] = get_relpath(point_1_path)
NEI_meta['SourceURL'] = 'http://eis.epa.gov'

#extract version from filepath using regex
import re
pattern = 'V[0-9]'
version = re.search(pattern,point_1_path,flags=re.IGNORECASE)
if version is not None:
    NEI_meta['SourceVersion'] = version.group(0)

#Write metadata to json
write_metadata('NEI',report_year, NEI_meta)

#VALIDATE
nei_national_totals = pd.read_csv(data_dir + 'NEI_'+ report_year + '_NationalTotals.csv',header=0,dtype={"FlowAmount[kg]":np.float})
# Rename col to match reference format
nei_national_totals.rename(columns={'FlowAmount[kg]':'FlowAmount'},inplace=True)
validation_result = validate_inventory(nei_flowbyfacility, nei_national_totals, group_by='flow', tolerance=5.0)
write_validation_result('NEI',report_year,validation_result)