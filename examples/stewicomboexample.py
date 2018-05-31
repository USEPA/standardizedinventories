import stewi
import stewicombo

stewi.seeAvailableInventoriesandYears()
#Enter inventories that you would like to combine in the "Inventory_acryonym":"year" format enclosed in "{}"
inventories_to_get = {"TRI":"2014","NEI":"2014","RCRAInfo":"2015","eGRID":"2014"}
#inventories_to_get = {"eGRID":"2014","GHGRP":"2014"}

inventories = stewicombo.combineInventories(inventories_to_get)

base_inventory = "eGRID"
inventories = stewicombo.combineInventoriesforFacilitiesinOneInventory(base_inventory, inventories_to_get)
inventories.to_csv('egrid2014_trircrainfoneiegrid.csv')

pivotofinventories =  stewicombo.pivotCombinedInventories(inventories)
pivotofinventories.to_csv('egrid2014_trircrainfoneiegrid_pivot.csv')



