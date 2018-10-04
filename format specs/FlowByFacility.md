## Flow-By-Facility Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
FlowName | String | Y | ID or name of flow in its native source
FlowAmount | Numeric | Y | The amount of a given flow released to a given environment compartment or waste generated in a reference unit. Uses metric reference units. 'kg' is the reference unit for mass; 'MJ' is the unit for energy. 
FacilityID | String | Y | a unique identification number used by the inventory to track the facility
ReliabilityScore | Numeric | Y | A score of data reliability based on reporting values associated with the amount see [US EPA Data Quality System](https://cfpub.epa.gov/si/si_public_record_report.cfm?dirEntryId=321834) and [Cashman et al. 2017](http://dx.doi.org/10.1021/acs.est.6b02160)
Compartment | String | Y | Name of compartment to which release goes, e.g. "air", "water", "ground". Used for inventory sources characterizing releases to multiple compartments.
Unit | String | Y | SI unit acronym. 'kg' for mass flows; 'MJ' for energy flows














