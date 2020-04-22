import facilitymatcher

#Get facility matches for TRI
facilitymatches = facilitymatcher.get_matches_for_inventories(["TRI"])
#See the first 50
facilitymatches.head(50)



