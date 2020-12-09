import pandas as pd
from stewi.globals import data_dir

list_for_filter = pd.read_excel(data_dir+'DMR/DMR_Parameter_List_ForFilter.xlsx', sheet_name='main',header=0)

#Just save the PARAMETER_DESC

flows_for_filter = pd.DataFrame(list_for_filter['PARAMETER_DESC'])
len(flows_for_filter)

flows_for_filter = flows_for_filter.rename(columns={'PARAMETER_DESC':'FlowName'})

flows_for_filter.to_csv(data_dir+'DMR_pollutant_omit_list.csv',index=False)