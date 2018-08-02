import facilitymatcher

facilitymatches = facilitymatcher.get_matches_for_inventories(["TRI"])

#Get matches only for list of facilities in one inventory
from_inventory = "eGRID"
to_inventories = ["TRI","NEI"]
facilitymatchesforoneinventory = facilitymatcher.get_table_of_matches_from_program_to_programs_of_interest(from_inventory,to_inventories)
#Peek at it
facilitymatchesforoneinventory.head()

#Count the matches
facilitymatcher.count_matches_from_inventory_to_inventories_of_interest(from_inventory,to_inventories)


