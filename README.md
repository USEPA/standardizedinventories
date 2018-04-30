# Standardized Emission and Waste Inventories (StEWI)
Provides processed EPA release and waste generation inventories in standard tabular formats. 
The standard output may be further aggregated or filtered based on given criteria. 

THIS CODE IS STILL IN EARLY DEVELOPMENT. OUTPUT FILES HAVE NOT YET BEEN TESTED.

## USEPA Inventories Covered By Data Reporting Year (current version)
|Source|2013|2014|2015|2016|
|--|--|--|--|--|
|[Discharge Monitoring Report](https://echo.epa.gov/tools/data-downloads/icis-npdes-dmr-and-limit-data-set)|||||
|[Greenhouse Gas Reporting Program](https://www.epa.gov/ghgreporting)|||x||
|[Toxic Release Inventory](https://www.epa.gov/toxics-release-inventory-tri-program)|x|x|x|x|
|[RCRA Biennial Report](https://www.epa.gov/hwgenerators/biennial-hazardous-waste-report)|x||x||
|[National Emissions Inventory](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei)||x|||

## FlowByFacility output format
|FacilityID|FlowName|FlowAmount|DataReliability|...optional fields|
|--|--|--|--|--|
| | | | | |

Each row represents the total amount of release or waste of a single type in a given year from the given facility.
Descriptions of the fields can be found [here](FlowByFacilityFormat.md)

## Use of the repository output
The standard format files without any filtering are available in the output directory in csv format (can be opened in Excel). These can be downloaded and used without knowledge of Python.
Within github, to get all files, select 'Clone or download', and you can download all files in the repository as a .zip.
To download an individual file, browse and click on the .csv file of interest, then click download. 
If the file is displayed in your browser you can use your browser's > File > Save as 
commands to save it, but make sure you use a .csv extension with no other extension added.

## Installation of python module
Use of Python permits further customization the output.
This repository contains a module `StandardizedReleaseandWasteInventories`. If you have Python 3.x installed, 
pip can be called to install the downloaded package. 

If you've downladed and unzipped the file, open the command line and type
>pip install -e `directory_of_unzipped_folder`

where `directory_of_unzipped_folder` is a directory like `C:/Users/username/lci-primer`
This will install the module as `StandardizedReleaseandWasteInventories`.

You can test the installation by opening up a Python console and entering
>> import StandardizedReleaseandWasteInventories

If no error code is returned, the module is installed

## Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis 
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer 
has responsibility to protect the integrity , confidentiality, or availability of the information. 
Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, 
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  
The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity 
by EPA or the United States Government.
