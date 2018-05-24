import stewi
import stewicombo

stewi.seeAvailableInventoriesandYears()
#Enter inventories that you would like to combine in the "Inventory_acryonym":"year" format enclosed in "{}"
inventories_to_get = {"TRI":"2014","NEI":"2014"}

inventories = stewicombo.combineInventories(inventories_to_get)

pivotofinventories =  stewicombo.pivotCombinedInventories(inventories)

