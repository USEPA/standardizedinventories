import stewi
import stewicombo

stewi.seeAvailableInventoriesandYears()

inventory='TRI'
year = '2016'

#Get one of these inventory
tri2016 = stewi.getInventory(inventory,year)
#See first 50
tri2016.head(50)

#Look at all the unique flows in this inventory
tri2016flows = stewi.getInventoryFlows(inventory,year)
#See first 50
tri2016flows.head(50)

#Look at all the unique facilities in this inventory
tri2016facilities = stewi.getInventoryFacilities(inventory,year)
#See first 50
tri2016facilities.head(50)

#Now combine with some inventories in another inventory based on facilities
#Enter inventories that you would like to combine in the "Inventory_acryonym":"year" format enclosed in "{}"
inventories_to_get = {"TRI":"2016","NEI":"2016","RCRAInfo":"2015","eGRID":"2016"}

base_inventory = inventory
combinedinventories = stewicombo.combineInventoriesforFacilitiesinOneInventory(base_inventory, inventories_to_get)
#See first 50
combinedinventories.head(50)

#See a summary of the combined inventories by facility and flow
pivotofinventories =  stewicombo.pivotCombinedInventories(combinedinventories)
pivotofinventories.head(200)



