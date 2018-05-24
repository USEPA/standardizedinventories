#NEI import and process to Standardized EPA output format
#This script uses the NEI data exports from EIS.

import stewi.globals as globals
import pandas as pd
import numpy as np

report_year = '2014'

output_dir = globals.output_dir
data_dir = globals.data_dir
nei_required_fields = pd.read_table(data_dir + 'NEI_required_fields.csv',sep=',').fillna('Null')
nei_file_path = pd.read_table(data_dir + 'NEI_' + report_year + '_file_path.csv',sep=',').fillna('Null')

def read_data(source,file):
    tmp = pd.Series(list(nei_required_fields[source]), index=list(nei_required_fields['StandardizedEPA']))
    file_result = pd.DataFrame(columns=[x for x in tmp[tmp!='Null'].index if x!='FlowAmount']+['sum'])
    # read nei file by chunks
    for file_chunk in pd.read_table(file,sep=',',usecols=list(set(nei_required_fields[source])-set(['Null'])),chunksize=100000,engine='python'):
        # change column names to Standardized EPA names
        file_chunk = file_chunk.rename(columns=pd.Series(list(nei_required_fields['StandardizedEPA']),index=list(nei_required_fields[source])).to_dict())
        # aggregate
        file_chunk = file_chunk.groupby([x for x in tmp[tmp!='Null'].index if x!='FlowAmount'])['FlowAmount'].agg(['sum']).reset_index()
        # concatenate all chunks
        file_result = pd.concat([file_result,file_chunk])
    return file_result.groupby(file_result.columns[:-1].tolist())['sum'].agg(['sum']).reset_index()

def standardize_output(source): # source as 'Point'/'NonPoint'/'OnRoad'/'NonRoad'
    # extract file paths
    file_path = list(set(nei_file_path[source]) - set(['Null']))
    # read in first nei file by chunks
    nei = read_data(source,file_path[0])
    print(file_path[0])
    print(len(nei))
    # read in other nei files and concatenate all nei files into one dataframe
    for file in file_path[1:]:
        # concatenate all other files
        nei = pd.concat([nei,read_data(source,file)])
        # aggregate
        nei = nei.groupby(nei.columns[:-1].tolist())['sum'].agg(['sum']).reset_index()
        print(file)
        print(len(nei))
    # convert LB/TON to KG
    nei['FlowAmount'] = np.where(nei['UOM']=='LB',nei['sum']*0.453592,nei['sum']*907.184)
    # add not included standardized columns as empty columns
    nei = pd.concat([nei,pd.DataFrame(columns=list(set(nei_required_fields['StandardizedEPA']) - set(nei.columns)))])
    nei = nei.fillna('')
    # add Reliability Score
    if source == 'Point':
        reliability_table = pd.read_csv(data_dir + 'DQ_Reliability_Scores_Table3-3fromERGreport.csv',usecols=['Source','Code','DQI Reliability Score'])
        nei_reliability_table = reliability_table[reliability_table['Source'] == 'NEI']
        nei_reliability_table['Code'] = nei_reliability_table.Code.astype(float)
        nei = nei.merge(nei_reliability_table, left_on='ReliabilityScore', right_on='Code', how='left')
        nei['ReliabilityScore'] = nei['DQI Reliability Score']
        # drop Code and DQI Reliability Score columns
        nei = nei.drop(['Code', 'DQI Reliability Score'], 1)
    else:
        nei['ReliabilityScore'] = 3
    # add Source column
    nei['Source'] = source
    # drop UOM and sum columns
    nei = nei.drop(['UOM','sum'],1)
    return(nei)

def nei_aggregate_unit_to_facility_level(nei_unit):

    #drop zeroes from flow amount
    nei_unit = nei_unit[nei_unit['FlowAmount'] > 0]

    grouping_vars = ['FacilityID', 'FlowName']

    #Do groupby with sum of flow amount and weighted avg of reliabilty
    #Too slow right now
    # Define a lambda function to compute the weighted mean
    wm = lambda x: np.average(x, weights=nei_unit.loc[x.index, "FlowAmount"])
    # Define a dictionary with the functions to apply for a given column:
    f = {'FlowAmount': ['sum'], 'ReliabilityScore': {'weighted_mean': wm}}
    # Groupby and aggregate with your dictionary:
    neibyfacility = nei_unit.groupby(grouping_vars).agg(f)

    #Procedure without weighted avg for the groupby
    #neibyfacility = nei_unit.groupby(grouping_vars)[['FlowAmount']]
    #neibyfacilityagg = neibyfacility.agg(sum)

    #Temp placeholder for now for reliability
    #neibyfacilityaggref['ReliabilityScore'] = 0

    neibyfacility = neibyfacility.reset_index()
    neibyfacility.columns = neibyfacility.columns.droplevel(level=1)

    return(neibyfacilityaggref)

#NEIPoint
nei_unit = standardize_output('Point')
len(nei_unit)

nei_facility = nei_aggregate_unit_to_facility_level(nei_unit)
len(nei_facility)

#Prepare unit for export
nei_unit = nei_unit.drop(columns=['Unitid','Unittype'])
nei_unit.to_csv(output_dir+'flowbyunit/'+'NEI_'+report_year+'.csv',index=False)

#Export flowbyfacility
nei_facility.to_csv(output_dir+'NEI_'+report_year+'.csv',index=False)

#Get flows
nei_flows = pd.DataFrame(pd.unique(nei_facility['FlowName']),columns=['FlowName'])
nei_flows.to_csv(output_dir+'flow/'+'NEI_'+report_year+'.csv',index=False)

#Needs a new format to output these data
#NEINonPoint
#nonpoint = standardize_output('NonPoint')
#NEIOnRoad
#onroad = standardize_output('OnRoad')
#NEINonRoad
#nonroad = standardize_output('NonRoad')
#nonpoint.to_csv(output_dir + 'NEINonPoint_2014.csv', index=False)
#onroad.to_csv(output_dir + 'NEIOnRoad_2014.csv', index=False)
#nonroad.to_csv(output_dir + 'NEINonRoad_2014.csv', index=False)
