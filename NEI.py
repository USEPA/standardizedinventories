#NEI import and process to Standardized EPA output format
#This script uses the NEI National Data File.

import pandas as pd

nei_required_fields = pd.read_table('./data/NEI_required_fields.csv',sep=',').fillna('Null')
nei_file_path = pd.read_table('./data/NEI_file_path.csv',sep=',').fillna('Null')

def read_data(file):
    tmp = pd.Series(list(nei_required_fields[source]), index=list(nei_required_fields['StandardizedEPA']))
    file_result = pd.DataFrame(columns=[x for x in tmp[tmp!='Null'].index if x!='Amount']+['sum'])
    # read nei file by chunks
    for file_chunk in pd.read_table(file,sep=',',usecols=list(set(nei_required_fields[source])-set(['Null'])),chunksize=100000,engine='python'):
        # change column names to Standardized EPA names
        file_chunk = file_chunk.rename(columns=pd.Series(list(nei_required_fields['StandardizedEPA']),index=list(nei_required_fields[source])).to_dict())
        # aggregate
        file_chunk = file_chunk.groupby([x for x in tmp[tmp!='Null'].index if x!='Amount'])['Amount'].agg(['sum']).reset_index()
        # concatenate all chunks
        file_result = pd.concat([file_result,file_chunk])
    return file_result.groupby(file_result.columns[:-1].tolist())['sum'].agg(['sum']).reset_index()

def standardize_output(source): # source as 'Point'/'NonPoint'/'OnRoad'/'NonRoad'
    # extract file paths
    file_path = list(set(nei_file_path[source]) - set(['Null']))

    # read in first nei file by chunks
    nei = read_data(file_path[0])
    print(len(nei))
    # read in other nei files and concatenate all nei files into one dataframe
    for file in file_path[1:]:
        # concatenate all other files
        nei = pd.concat([nei,read_data(file)])
        # aggregate
        nei = nei.groupby(nei.columns[:-1].tolist())['sum'].agg(['sum']).reset_index()
        print(file)
        print(len(nei))
    # change column name
    nei.rename(columns={'sum':'Amount'},inplace=True)
    return(nei)

