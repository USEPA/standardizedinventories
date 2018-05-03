#NEI import and process to Standardized EPA output format
#This script uses the NEI National Data File.

import StandardizedReleaseAndWasteInventories.globals as globals
import pandas as pd
import numpy as np
import math

report_year = '2011'

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
    grouping_vars = ['FacilityID','NAICS','FlowName','State']
    neibyfacility = nei_unit.groupby(grouping_vars)[['FlowAmount']]

    neibyfacilityagg = neibyfacility.agg([('FlowAmount','sum')])
    neibyfacilityagg = neibyfacility.agg(sum)

    neibyfacilityaggref = neibyfacilityagg.reset_index()
    return(neibyfacilityaggref)

#NEIPoint
nei_unit = standardize_output('Point')
nei_facility = nei_aggregate_unit_to_facility_level(nei_unit)

#For use in NEI flowbyunitprocess
#Needs revision
#point_row = math.floor(len(point)/6)
#break down NEIPoint into 6 parts
#point1 = point.iloc[:point_row,]
#point2 = point.iloc[point_row:point_row*2,]
#point3 = point.iloc[point_row*2:point_row*3,]
#point4 = point.iloc[point_row*3:point_row*4,]
#point5 = point.iloc[point_row*4:point_row*5,]
#point6 = point.iloc[point_row*5:,]

#NEINonPoint
nonpoint = standardize_output('NonPoint')
#NEIOnRoad
onroad = standardize_output('OnRoad')
#NEINonRoad
nonroad = standardize_output('NonRoad')

#Output to CSV
#point1.to_csv(output_dir + 'NEIPoint1_2014.csv', index=False)
#point2.to_csv(output_dir + 'NEIPoint2_2014.csv', index=False)
#point3.to_csv(output_dir + 'NEIPoint3_2014.csv', index=False)
#point4.to_csv(output_dir + 'NEIPoint4_2014.csv', index=False)
#point5.to_csv(output_dir + 'NEIPoint5_2014.csv', index=False)
#point6.to_csv(output_dir + 'NEIPoint6_2014.csv', index=False)

nei_facility.to_csv(output_dir+'NEI_'+report_year+'.csv')

nonpoint.to_csv(output_dir + 'NEINonPoint_2014.csv', index=False)
onroad.to_csv(output_dir + 'NEIOnRoad_2014.csv', index=False)
nonroad.to_csv(output_dir + 'NEINonRoad_2014.csv', index=False)
