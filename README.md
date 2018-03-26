# Standardized Release and Waste Inventories
Provides processed EPA release and waste generation inventories in a standard tabular format.

THIS CODE IS STILL IN EARLY DEVELOPMENT. OUTPUT FILES HAVE NOT YET BEEN TESTED.

## USEPA Inventories Covered By Data Reporting Year (current version)
|Source|2014|2015|2016|
|--|--|--|--|
|[Discharge Monitoring Report](https://echo.epa.gov/tools/data-downloads/icis-npdes-dmr-and-limit-data-set)|x|||
|[Greenhouse Gas Reporting Program](https://www.epa.gov/ghgreporting)|x|||
|[Toxic Release Inventory](https://www.epa.gov/toxics-release-inventory-tri-program)|x|||
|[RCRA Biennial Report](https://www.epa.gov/hwgenerators/biennial-hazardous-waste-report)||x||
|[National Emissions Inventory](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei)|x|||

## FlowByFacility output format
|Facility ID|Flow ID|Compartment|Flow Amount|Data Reliability|...optional fields|
|--|--|--|--|--|--|

Each row represents the total amount of release or waste of a single type in a given year from the given facility.
Definitions:
`Facility ID` is a unique identification number used by the emission or waste inventory source to track the facility.
`Flow ID` is the unique identification number for the release or waste given by the source.
`Flow Amount` is the amount of a given flow released to a given environment compartment (air, water, or ground) for the given year by that facility. 
The `Data Reliability` score is a 1-5 score. The scoring method uses an [EPA data quality assessment protocol](https://cfpub.epa.gov/si/si_public_record_report.cfm?dirEntryId=321834). Scores are based on the data describing o releast.
A rubric for assigning the score for each source is provided here.
These and other fields are defined in this [file](data/Standarized_Output_Format_EPA%20_Data_Sources.csv).

## Use of the repository
The standard format files are available in the output directory in csv format (can be opened in Excel). The Python code can by used or modified in any Python 3.x environment. 

## Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer has responsibility to protect the integrity , confidentiality, or availability of the information.  Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
