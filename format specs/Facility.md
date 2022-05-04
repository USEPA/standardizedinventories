## Facility Format

Field | Type | Required? | Description|
----- | ---- | --------  | -----------|
FacilityID | String | Y | a unique identification number used by the inventory to track the facility|
FacilityName |String |N | Name of the facility given by the inventory source  |
Address |String| N| Address of the facility in the inventory source|
City |String|N| City of the facility in the inventory source|
State | String | Y | Two-letter US postal acronymn for facility|
Zip |Integer|N| 5 digit USPS ZIP Code|
Latitude |Numeric|N|Latitude in decimal degrees|
Longitude |Numeric|N|Longitude in decimal degrees|
County |String|N|County Name|
NAICS |String|N|[NAICS code](https://www.census.gov/cgi-bin/sssd/naics/naicsrch?chart=2012), NAICS 2012 is assumed.|
SIC |String|N|[Standard Industry Classification (1987)](https://www.osha.gov/pls/imis/sicsearch.html)|
UrbanRural | String | N | Population density of the facility*: `urban`, `rural`, or `unspecified`

Additional fields specific to individual inventories are maintained in some cases.
- eGRID [facility fields](https://github.com/USEPA/standardizedinventories/blob/master/stewi/data/eGRID/eGRID_required_fields.csv)

* Optionally assigned by esupy if [geospatial packages are installed](https://github.com/USEPA/standardizedinventories#secondary-context-installation-steps).
