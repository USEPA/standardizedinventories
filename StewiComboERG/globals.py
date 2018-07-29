DATA_FILEPATH = "data/StewiComboInputNew.xlsx"
LOOKUP_FIELDS = ["FRS_ID", "Compartment", "SRS_ID"]
OUTPUT_FILEPATH = "output.csv"
KEEP_ALL_DUPLICATES =  True
INCLUDE_ORIGINAL =  True
SOURCE_COL = "Source"
COMPARTMENT_COL = "Compartment"
COL_FUNC_PAIRS = {
    "FacilityID": "join_with_underscore",
    "FlowAmount": "sum",
    "ReliabilityScore": "reliablity_weighted_sum:FlowAmount"
}
COL_FUNC_DEFAULT = "get_first_item"
INVENTORY_PREFERENCE_BY_COMPARTMENT = {
    "air": ["eGRID","GHGRP","NEI","TRI"],
    "water": ["DMR", "TRI"],
    "soil": ["TRI"],
    "waste": ["RCRAInfo","TRI"]
}
