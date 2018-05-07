## Flow-By-Unit Process Format
Provides totals by a unit operating in an EPA facility. Currently only applies to NEI.

Field | Type | Required? | Description
----- | ---- | --------  | -----------
FlowName | String | Y | ID or name of flow in its native source
FlowAmount | Numeric | Y | The amount of a given flow released to a given environment compartment or waste generated in a reference unit. Uses metric reference units. 'kg' is the reference unit for mass; 'MJ' is the unit for energy.
FacilityID | String | Y | a unique identification number used by the inventory to track the facility
ReliabilityScore | Integer | Y | A score of data reliability based on reporting values associated with the amount see [US EPA Data Quality System](https://cfpub.epa.gov/si/si_public_record_report.cfm?dirEntryId=321834) and [Cashman et al. 2017](http://dx.doi.org/10.1021/acs.est.6b02160)
SCC | String | N | [EPA Source Classification Codes](https://ofmpub.epa.gov/sccsearch/)
