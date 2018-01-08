LCI-Primer produces LCI in table form with standard fields. The field names are constant but ordering may vary depending on which fields are provided.

#### Standard Fields
Field | Type | Required? | Description
----- | ---- | --------  | -----------
OriginalFlowID | String | Y | ID or name of flow in its native source
Amount | Numeric | Y | Amount in a reference unit. Uses [openLCA](http://www.openlca.org/) reference units. 'kg' is the reference unit for mass; 'MJ' is the unit for energy. 
FacilityID | String | Y | ID of facility in EPA system
ReliabilityScore | Integer | Y | A score of data reliability based on reporting values associated with the amount see [US EPA Data Quality System](https://cfpub.epa.gov/si/si_public_record_report.cfm?dirEntryId=321834) and [Cashman et al. 2017](http://dx.doi.org/10.1021/acs.est.6b02160)
State | String | Y | Two-letter US state acronymn for facility
Context | String | N | Name of compartment to which release goes, e.g. "air", "water", "soil". Used for inventory sources characterizing multiple releases  
NAICS | String | N  | [NAICS 2012 code](https://www.census.gov/cgi-bin/sssd/naics/naicsrch?chart=2012)
SIC | String | N | [Standard Industry Classification (1987)](https://www.osha.gov/pls/imis/sicsearch.html)
SCC | String | N | [EPA Source Classification Codes](https://ofmpub.epa.gov/sccsearch/)










