import pandas as pd
from stewicombo.globals import *

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

    print("Aggregating inventories...")

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
    for name, frame in grouped:
        # print(name, df)
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
        # print(df_new)
        # print(name)
        to_be_concat.append(df_new)

    df = pd.concat(to_be_concat)

    print("Adding any rows with NaN FRS_ID or SRS_ID")
    df = df.append(rows_with_nans_srs_frs, ignore_index=True)
    
    #Find records where SRS_ID = 77683 (PM10-PRI) and SRS_ID = 77681  (PM2.5-PRI) and the same FRS_ID. The record with SRS_ID 77681 is not changed.
	#In the record with SRS_ID 77683 (PM10-PRI) change FlowAmount by subtracting amount in record 77681 
	#Add column CombinedFlowLookup to output and concatenate 
	#Find records from same FRS_ID where SRS_ID = 83723 (Volatile organic compounds(VOC)) and records with speciated HAP VOCs
    #defined in EPA’s Industrial, Commercial, and Institutional (ICI) Fuel Combustion Tool, Version 1.4, December 2015
    #(Available at: ftp://ftp.epa.gov/EmisInventory/2014/doc/nonpoint/ICI%20Tool%20v1_4.zip).
    #SRS_IDs for speciated HAP VOCs = 47917,78938,94770,47892,94767,47850,78931,78930,39151,78937,78935,47830,47823,95636,
    #78945,47528,47526,47524,47520,47519,47498,96714,47495,47490,47488,47486,47485,47480,47475,47472,96461,47428,47417,
    # 96452,47414,47380,47379,47376,47321,47687,47684,47669,47661,46935,46932,46916,46914,47310,96237,47301,96044,43483,
    # 96033,96030,47282,96026,47276,47275,96023,47265,47264,47202,95786,95501,95480,47040,95217,95212,47018,47008,47007,
    # 46998,46986,94909,47248,47227,47211,94706,46821,46802,46756,46705,84476,97998,43230,46646,43047,46639,16366,46629,
    # 78926,97768,46502,46377,93632,82787,41401,60034,35858,95408,62450,46354,46353,91912,46351,46350,91750,46349,91722,
    # 78959,46348,78943,94309,81662,93573,81661,46346,93562,46345,82597,93556,46344,93021,46342,92639,56016,56009,92240,
    # 78460,35886,78457,106307,56172,56134,79079,93149,78946,41708,96236,59307,91450,46205,91779,78934,6012,78852,93878,
    # 78947,82763,42624,45878,49201,49190,46053,49177,49165,45685,45599,49154,51117,49152,88529,45499,45495,38608,90163,
    # 48965,48961,48958,48956,51299,48940,49142,61763,78217,89262,45190,49118,45076,49102,49098,90653,49090,49089,44962,
    # 44772,44702,49067,89211,49062,44603,49061,49050,78853,49030,49023,49020,49017,44344,49011,44334,44310,87338,98385,
    # 48560,88479,48547,48543,48542,78944,6011,48532,48531,48527,48526,48521,48517,48938,36616,48932,48931,87890,39509,
    # 87885,48922,87744,78932,44174,48812,48799,44120,78933,48764,48750,48746,48739,48738,48733,48729,48728,48715,48709,
    # 48708,87298,35041,48666,44441,98352,43687,48599,48873,98155,48853,48444,48443,48415,48409,48401,48311,48309,48273,
    # 87972,87964,48260,48246,48214,87829,48195,87812,78936,48475,87536,47752,47748,47729,47719,48148,48146,48129,87374,
    # 47969,47967,87268
    # In the record with SRS_ID = 83723 (VOC) change FlowAmount by subtracting sum of FlowAmount from speciated HAP VOCs.
    # The records for speciated HAP VOCs are not changed.

    #df_83723 = df.loc[df["SRS_ID"] == 83723]

    df_77681 = df.loc[df["SRS_ID"] == '77681']

    for i, row in df_77681.iterrows():
        ids = (df["SRS_ID"] == '77683') & (df["FRS_ID"] == row["FRS_ID"])
        df.loc[ids, "FlowAmount"] -= row["FlowAmount"]
        df.loc[ids, "CombinedFlowLookup"] = "SRS_" + df.loc[ids, "SRS_ID"].astype(str) + "_" + \
                                            df.loc[ids, "Source"] + "_" + \
                                            df.loc[ids, "Compartment"] + "_" + \
                                            df.loc[ids, "FRS_ID"].astype(str)

        # concatenate for the comparing row with SRS_ID 77681 as well
        ids = (df["SRS_ID"] == '77681') & (df["FRS_ID"] == row["FRS_ID"])
        df.loc[ids, "CombinedFlowLookup"] = "SRS_" + df.loc[ids, "SRS_ID"].astype(str) + "_" + \
                                            df.loc[ids, "Source"] + "_" + \
                                            df.loc[ids, "Compartment"] + "_" + \
                                        df.loc[ids, "FRS_ID"].astype(str)
    '''
    srs_ids = [47917,78938,94770,47892,94767,47850,78931,78930,39151,78937,78935,47830,47823,95636,78945,47528,47526,47524,47520,47519,47498,96714,47495,47490,47488,47486,47485,47480,47475,47472,96461,47428,47417,96452,47414,47380,47379,47376,47321,47687,47684,47669,47661,46935,46932,46916,46914,47310,96237,47301,96044,43483,96033,96030,47282,96026,47276,47275,96023,47265,47264,47202,95786,95501,95480,47040,95217,95212,47018,47008,47007,46998,46986,94909,47248,47227,47211,94706,46821,46802,46756,46705,84476,97998,43230,46646,43047,46639,16366,46629,78926,97768,46502,46377,93632,82787,41401,60034,35858,95408,62450,46354,46353,91912,46351,46350,91750,46349,91722,78959,46348,78943,94309,81662,93573,81661,46346,93562,46345,82597,93556,46344,93021,46342,92639,56016,56009,92240,78460,35886,78457,106307,56172,56134,79079,93149,78946,41708,96236,59307,91450,46205,91779,78934,6012,78852,93878,78947,82763,42624,45878,49201,49190,46053,49177,49165,45685,45599,49154,51117,49152,88529,45499,45495,38608,90163,48965,48961,48958,48956,51299,48940,49142,61763,78217,89262,45190,49118,45076,49102,49098,90653,49090,49089,44962,44772,44702,49067,89211,49062,44603,49061,49050,78853,49030,49023,49020,49017,44344,49011,44334,44310,87338,98385,48560,88479,48547,48543,48542,78944,6011,48532,48531,48527,48526,48521,48517,48938,36616,48932,48931,87890,39509,87885,48922,87744,78932,44174,48812,48799,44120,78933,48764,48750,48746,48739,48738,48733,48729,48728,48715,48709,48708,87298,35041,48666,44441,98352,43687,48599,48873,98155,48853,48444,48443,48415,48409,48401,48311,48309,48273,87972,87964,48260,48246,48214,87829,48195,87812,78936,48475,87536,47752,47748,47729,47719,48148,48146,48129,87374,47969,47967,87268]

    for i, row in df_83723.iterrows():
        for srs_id in srs_ids:
            print("Processing  SRS_ID  " + str(srs_id))
            ids = (df["SRS_ID"] == srs_id) & (df["FRS_ID"] == row["FRS_ID"])
            df.loc[ids, "FlowAmount"] -= row["FlowAmount"]
            df.loc[ids, "CombinedFlowLookup"] = "SRS_" + df.loc[ids, "SRS_ID"].astype(str) + "_" + \
                                                df.loc[ids, "Source"] + "_" + \
                                                df.loc[ids, "Compartment"] + "_" + \
                                                df.loc[ids, "FRS_ID"].astype(str)


        # concatenate for comparing row with SRS_ID 83723 as well
        ids = (df["SRS_ID"] == 83723) & (df["FRS_ID"] == row["FRS_ID"])
        df.loc[ids, "CombinedFlowLookup"] = "SRS_" + df.loc[ids, "SRS_ID"].astype(str) + "_" + \
                                            df.loc[ids, "Source"] + "_" + \
                                            df.loc[ids, "Compartment"] + "_" + \
                                            df.loc[ids, "FRS_ID"].astype(str)
    '''
    # drop QA column
    if 'CombinedFlowLookup' in df.columns:
        df.drop(columns=['CombinedFlowLookup'],inplace=True)
    
    #  end PM and VOC handler
    
    print("Overlap removed.")
    return df
