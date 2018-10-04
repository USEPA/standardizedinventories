import re
#Variables and functions for common use
INVENTORY_PREFERENCE_BY_COMPARTMENT = {"air":["eGRID","GHGRP","NEI","TRI"],
                                       "water":["DMR", "TRI"],
                                       "soil":["TRI"],
                                       "waste":["RCRAInfo","TRI"]}

LOOKUP_FIELDS = ["FRS_ID", "Compartment", "SRS_ID"]
# pandas might infer wrong type, force cast skeptical columns
FORCE_COLUMN_TYPES = {
    "SRS_CAS": "str"
}

KEEP_ALL_DUPLICATES =  True
INCLUDE_ORIGINAL =  True
KEEP_ROW_WITHOUT_DUPS = True
SOURCE_COL = "Source"
COMPARTMENT_COL = "Compartment"
COL_FUNC_PAIRS = {
    "FacilityID": "join_with_underscore",
    "FlowAmount": "sum",
    "ReliabilityScore": "reliablity_weighted_sum:FlowAmount"
}
COL_FUNC_DEFAULT = "get_first_item"

#Remove substring from inventory name
def get_id_before_underscore(inventory_id):
    underscore_match = re.search('_', inventory_id)
    if underscore_match is not None:
        inventory_id = inventory_id[0:underscore_match.start()]
    return inventory_id
