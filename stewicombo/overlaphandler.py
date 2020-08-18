import pandas as pd
from stewicombo.globals import *
from stewi.globals import log

if not "LOOKUP_FIELDS" in locals() and LOOKUP_FIELDS:
    raise ValueError("Not sure which fields to lookup in each row. Please update config.json with LOOKUP_FIELDS")


def join_with_underscore(items):
    type_cast_to_str = False
    for x in items:
        if not isinstance(x, str):
            # raise TypeError("join_with_underscore()  inputs must be string")
            type_cast_to_str = True
    if type_cast_to_str:
        items = [str(x) for x in items]

    return "_".join(items)

def reliablity_weighted_sum(df, weights_col_name, items):
    grouped = df.groupby(SOURCE_COL)

    for x, y in items.items():
        first_index = x
        break

    # group_name = df.iloc[first_index].loc[SOURCE_COL]
    group_name = df.loc[first_index, SOURCE_COL]
    group = grouped.get_group(group_name)

    new_reliability_col = items * (group[weights_col_name] / sum(group[weights_col_name]))
    return sum(new_reliability_col)

def get_first_item(items):
    return items.iloc[0]

def get_by_preference(group):
    preferences = INVENTORY_PREFERENCE_BY_COMPARTMENT[group.name]

    for pref in preferences:
        for index, row in group.iterrows():
            if pref == row[SOURCE_COL]:
                return row



def aggregate_and_remove_overlap(df):
    if not INCLUDE_ORIGINAL and not KEEP_ALL_DUPLICATES:
        raise ValueError("Cannot have both INCLUDE_ORIGINAL and KEEP_REPEATED_DUPLICATES fields as False")

    log.info("Aggregating inventories...")

    if INCLUDE_ORIGINAL:
        keep = False
    else:
        keep = 'first'

    # force cast skeptical columns
    for col_name, dtype in FORCE_COLUMN_TYPES.items():
        df[col_name] = df[col_name].astype(dtype)

    # 2
    # if you wish to also keep row that doesn't have any duplicates, don't find duplicates
    # go ahead with next step of processing
    if not KEEP_ROW_WITHOUT_DUPS:

        df_chunk_filtered = df[LOOKUP_FIELDS]

        if not KEEP_ALL_DUPLICATES:
            # from a set of duplicates a logic is applied to figure out what is sent to write to output file
            # for example only the first duplicate is kept
            # or duplicates are filtered preferentially and high priority one is kept etc
            df_dups = df[df_chunk_filtered.duplicated(keep=keep)]
            df_dups_filtered = df_dups[LOOKUP_FIELDS]
            df = df_dups[df_dups_filtered.duplicated(keep=keep).apply(lambda x: not x)]

    # 3
    # if any row has FRS_ID or SRS_ID as NaN, extract them and add to the output
    rows_with_nans_srs_frs = df[df.loc[:, "FRS_ID"].isnull() | df.loc[:, "SRS_ID"].isnull()]
    # print(rows_with_nans_srs_frs)

    # remaining rows
    df = df[~(df.loc[:, "FRS_ID"].isnull() | df.loc[:, "SRS_ID"].isnull())]

    #print("Grouping duplicates by LOOKUP_FIELDS")
    grouped = df.groupby(LOOKUP_FIELDS)

    #print("Grouping duplicates by SOURCE_COL")
    if SOURCE_COL not in df.columns: raise ("SOURCE_COL not found in input file's header")


    #print("Combining each group to a single row")
    funcname_cols_map = COL_FUNC_PAIRS
    for col in list(set(df.columns) - set(
            COL_FUNC_PAIRS.keys())):  # col names in columns, not in key of COL_FUNC_PAIRS
        funcname_cols_map[col] = COL_FUNC_DEFAULT

    to_be_concat = []
    # Check if frame includes more than one record before using the function key mapping
    group_length = len(grouped)
    counter = 1
    pct = 1
    for name, frame in grouped:
        if len(frame)==1:
            to_be_concat.append(frame)
        else:
            # find functions mapping for this df
            func_cols_map = {}
            for key, val in funcname_cols_map.items():
                if "reliablity_weighted_sum" in val:
                    args = val.split(":")
                    if len(args) > 1:
                        weights_col_name = args[1]
                    func_cols_map[key] = lambda items: reliablity_weighted_sum(frame, weights_col_name, items)
                else:
                    func_cols_map[key] = eval(val)
            grouped_by_src = frame.groupby(SOURCE_COL)
            df_new = grouped_by_src.agg(func_cols_map)
    
            # If we have 2 or more duplicates with same compartment use `INVENTORY_PREFERENCE_BY_COMPARTMENT`
            grouped = df_new.groupby(COMPARTMENT_COL)
            df_new = grouped.apply(get_by_preference)
            to_be_concat.append(df_new)
        if counter / group_length >= 0.1*pct:
            log.info(str(pct) +'0% completed')
            pct +=1
        counter+=1
    df = pd.concat(to_be_concat)

    log.info("Adding any rows with NaN FRS_ID or SRS_ID")
    df = df.append(rows_with_nans_srs_frs, ignore_index=True)
    
    log.info("Assessing PM and VOC speciation")

    # SRS_ID = 77683 (PM10-PRI) and SRS_ID = 77681  (PM2.5-PRI)
    df = remove_flow_overlap(df, '77683',['77681'])
    
    # SRS_ID = 83723 (VOC) change FlowAmount by subtracting sum of FlowAmount from speciated HAP VOCs.
    # The records for speciated HAP VOCs are not changed.
    # Defined in EPA’s Industrial, Commercial, and Institutional (ICI) Fuel Combustion Tool, Version 1.4, December 2015
    # (Available at: ftp://ftp.epa.gov/EmisInventory/2014/doc/nonpoint/ICI%20Tool%20v1_4.zip).
    df = remove_flow_overlap(df, '83723',VOC_srs)
   
    log.info("Overlap removed.")
    return df

def remove_flow_overlap(df, aggregate_flow, contributing_flows, compartment='air', SCC=False):
    
    df_contributing_flows = df.loc[df["SRS_ID"].isin(contributing_flows)]
    df_contributing_flows = df_contributing_flows[df_contributing_flows['Compartment']==compartment]
    match_conditions = ['FacilityID','Source','Compartment']
    if SCC:
        match_conditions.append('SCC')
    df_contributing_flows = df_contributing_flows.groupby(match_conditions, as_index=False)['FlowAmount'].sum()
    
    for i, row in df_contributing_flows.iterrows():
        if SCC:
            ids = (df["SRS_ID"] == aggregate_flow) & (df["FacilityID"] == row["FacilityID"]) & (df["Source"] == row["Source"]) & (df["Compartment"] == row["Compartment"]) & (df["SCC"] == row["SCC"])
        else:
            ids = (df["SRS_ID"] == aggregate_flow) & (df["FacilityID"] == row["FacilityID"]) & (df["Source"] == row["Source"]) & (df["Compartment"] == row["Compartment"])
        df.loc[ids, "FlowAmount"] -= row["FlowAmount"]
        #TODO make sure values are not negative
   
    return df

if __name__ == '__main__':
    import stewicombo
    nei_df = stewicombo.combineFullInventories({"NEI":"2016"},remove_overlap=False)
    nei_df = nei_df.head(10000)
    nei_df = aggregate_and_remove_overlap(nei_df)
    #nei_df = remove_flow_overlap(nei_df,'83723',VOC_srs)
