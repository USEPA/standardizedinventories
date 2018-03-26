# Standardized Release and Waste Inventories
Provides processed EPA release and waste generation inventories in standard tabular formats. 
These inventories may be further aggregated or filtered based on given criteria.

THIS CODE IS STILL IN EARLY DEVELOPMENT. OUTPUT FILES HAVE NOT YET BEEN TESTED.

## USEPA Inventories Covered By Data Reporting Year (current version)
|Source|2013|2014|2015|2016|
|--|--|--|--|
|[Discharge Monitoring Report](https://echo.epa.gov/tools/data-downloads/icis-npdes-dmr-and-limit-data-set)|||||
|[Greenhouse Gas Reporting Program](https://www.epa.gov/ghgreporting)|||x||
|[Toxic Release Inventory](https://www.epa.gov/toxics-release-inventory-tri-program)|x|x|x|x|
|[RCRA Biennial Report](https://www.epa.gov/hwgenerators/biennial-hazardous-waste-report)|||x||
|[National Emissions Inventory](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei)||x|||

## FlowByFacility output format
|FacilityID|FlowName|FlowAmount|DataReliability|...optional fields|
|--|--|--|--|--|
| | | | | |

Each row represents the total amount of release or waste of a single type in a given year from the given facility.
Descriptions of the fields can be found [here](FlowByFacilityFormat.md)

## Use of the repository
The standard format files without any filtering are available in the output directory in csv format (can be opened in Excel). The Python code can by used or modified in any Python 3.x environment. 

## Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer has responsibility to protect the integrity , confidentiality, or availability of the information.  Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
