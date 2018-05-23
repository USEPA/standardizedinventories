import pandas as pd
import os
import logging
import stewi

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'stewicombo/'

def combineInventories(inventory_dict):
    inventories = pd.DataFrame()
    for k in inventory_dict.keys():
        inventory = stewi.getInventory(k,inventory_dict[k],include_optional_fields=False)
        inventory["Source"] = k
        inventories = pd.concat([inventories,inventory])

