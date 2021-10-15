import stewi
import stewicombo


def main():
    stewi.seeAvailableInventoriesandYears()

    inventory = 'TRI'
    year = '2016'

    # Get one of these inventory, if the inventory does not exist locally it will
    # be created
    tri2016 = stewi.getInventory(inventory, year)

    # Review and apply available inventory filters
    stewi.seeAvailableInventoryFilters()
    tri2016_filtered = stewi.getInventory(inventory, year,
                                          filters=['US_States_only'])

    # Look at all the unique flows in this inventory
    tri2016flows = stewi.getInventoryFlows(inventory, year)

    # Look at all the unique facilities in this inventory
    tri2016facilities = stewi.getInventoryFacilities(inventory, year)

    # Now combine with some inventories in another inventory based on facilities
    # Enter inventories that you would like to combine in the
    # format of a dictionary
    inventories_to_get = {"TRI": "2016", "NEI": "2016",
                          "RCRAInfo": "2015", "eGRID": "2016"}

    base_inventory = inventory
    combinedinventories = stewicombo.combineInventoriesforFacilitiesinBaseInventory(
        base_inventory, inventories_to_get)

    # Store the combined inventory and metadata to local user directory
    stewicombo.saveInventory('MyCombinedInventory', combinedinventories,
                             inventories_to_get)

    # See a summary of the combined inventories by facility and flow
    pivotofinventories = stewicombo.pivotCombinedInventories(combinedinventories)
    pivotofinventories.head(200)


if __name__ == "__main__":
    main()
