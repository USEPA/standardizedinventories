## Facility Format

Field | Type | Required? | Description|
----- | ---- | --------  | -----------|
FacilityID | String | Y | a unique identification number used by the inventory to track the facility
FacilityName |String | | |
Address |String|||
City |String|||
State | String | Y | Two-letter US state acronymn for facility
Zip |Integer|5 digit||
Latitude |Numeric|||
Longitude |Numeric|||
County |String|County Name||
NAICS |String| [NAICS code](https://www.census.gov/cgi-bin/sssd/naics/naicsrch?chart=2012), NAICS 2012 is assumed.||
SIC |String|[Standard Industry Classification (1987)](https://www.osha.gov/pls/imis/sicsearch.html)||


