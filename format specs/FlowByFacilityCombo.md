## Flow-By-Facility Combined Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
FlowName | String | Y | ID or name of flow in its native source
FlowAmount | Numeric | Y | The amount of a given flow released to a given environment compartment or waste generated in a reference unit. Uses metric reference units. 'kg' is the reference unit for mass; 'MJ' is the unit for energy. 
FacilityID | String | Y | a unique identification number used by the inventory to track the facility
DataReliability | Numeric | Y | A score of data reliability based on reporting values associated with the amount see [US EPA Data Quality System](https://cfpub.epa.gov/si/si_public_record_report.cfm?dirEntryId=321834) and [Cashman et al. 2017](http://dx.doi.org/10.1021/acs.est.6b02160)
Compartment | String | Y | Name of compartment to which release goes, e.g. "air", "water", "ground". Used for inventory sources characterizing releases to multiple compartments.
Unit | String | Y | SI unit acronym. 'kg' for mass flows; 'MJ' for energy flows
Source |String |Y | The standard acronym for the inventory that includes this flow|
Year | String | Y | The year of the inventory
SRS_ID |String |N | The [Substance Registry Service (FRS)](https://iaspub.epa.gov/sor_internet/registry/substreg/home/overview/home.do) substance ID number |
SRS_CAS | String | N | The [Substance Registry Service (FRS)](https://iaspub.epa.gov/sor_internet/registry/substreg/home/overview/home.do) substance CAS number | 
FRS_ID |String |N | The [Facility Registry Service (FRS)](https://iaspub.epa.gov/sor_internet/registry/facilreg/home/basicinformation/) facility ID number |
FacilityIDs_Combined | String | N | Concatenated FacilityID when FRS_ID is shared
%Source%_ID|String| N | The FacilityID number of the base inventory source used for selecting facilities to be combined | 


