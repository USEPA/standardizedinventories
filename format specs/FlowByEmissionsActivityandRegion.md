## Flow-By-Emissions Activity and Region Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
FlowName | String | Y | ID or name of flow in its native source
FlowAmount | Numeric | Y | The amount of a given flow released to a given environment compartment or waste generated in a reference unit. Uses metric reference units. 'kg' is the reference unit for mass; 'MJ' is the unit for energy. 
SCC | String | Y | EPA [Source Classification Codes](https://ofmpub.epa.gov/sccwebservices/sccsearch/)
FIPS | String | Y | Place Codes specified by ANSI for US regions. Two digits codes are state; Five digit codes are counties. [Reference](https://www.census.gov/library/reference/code-lists/ansi.html)   
ReliabilityScore | Numeric | Y | A score of data reliability based on reporting values associated with the amount see [US EPA Data Quality System](https://cfpub.epa.gov/si/si_public_record_report.cfm?dirEntryId=321834) and [Cashman et al. 2017](http://dx.doi.org/10.1021/acs.est.6b02160)
Compartment | String | Y | Name of compartment to which release goes, e.g. "air", "water", "ground". Used for inventory sources characterizing releases to multiple compartments.
Unit | String | Y | SI unit acronym. 'kg' for mass flows; 'MJ' for energy flows

Emissions by EPA's source classification codes within regions, using ANSI (which are FIPS codes).