# Standardized Emission and Waste Inventories (StEWI)
Provides processed EPA emission and waste generation inventories in standard tabular formats. The standard outputs may be
 further aggregated or filtered based on given criteria, and can be combined based on common facility and flows
  across the inventories. 

StEWI consists of a core module, `stewi`, that digests and provides the USEPA inventory data in standard formats. Two matcher modules, the `facilitymatcher` 
and `chemicalmatcher`, provide commons IDs for facilities and flows across inventories, which is used by the `stewicombo` module
to combine the data, and optionally remove overlaps and remove double counting of groups of chemicals based on user preferences.

## USEPA Inventories Covered By Data Reporting Year (current version)
|Source|2011|2012|2013|2014|2015|2016|
|--|--|--|--|--|--|--|
|[Toxic Release Inventory](https://www.epa.gov/toxics-release-inventory-tri-program)|x|x|x|x|x|x|
|[RCRA Biennial Report](https://www.epa.gov/hwgenerators/biennial-hazardous-waste-report)|x||x||x||
|[National Emissions Inventory](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei)*|x|||x||x|
|[Emissions & Generation Resource Integrated Database](https://www.epa.gov/energy/emissions-generation-resource-integrated-database-egrid)||||x||x|

*Only point sources included at this time from NEI

## Current output formats
The core `stewi` module produces the following output formats:
[Flow-By-Facility](./format%20specs/FlowByFacility.md): Each row represents the total amount of release or waste of a single type in a given year from the given facility.
[Facility](./format%20specs/Facility.md): Each row represents a unique facility in a given inventory and given year
[Flow](./format%20specs/Flow.md):  Each row represents a unique flow (substance or waste) in a given inventory and given year
The `chemicalmatcher` module produces:
[Chemical Matches](./format%20specs/ChemicalMatches.md): Each row provides a common identifier for an inventory flow chemical
The `facilitymatcher` module produces:
[Facility Matches](./format%20specs/FacilityMatches.md): Each row provides a common identifier for an inventory facility
The `stewicombo` module produces:
[Flow-By-Facility Combined](./format%20specs/FlowByFacilityCombined.md): Analagous to the flowbyfacility, with chemical and facilitymatches added 


## Use of the repository output
The standard format files without any filtering are available in the module output directories in csv format (can be opened in Excel).
 These can be downloaded and used without knowledge of Python. Within github, to get all files, select 'Clone or download', and you can download all files in the repository as a .zip.
To download an individual file, browse and click on the .csv file of interest, then click download. 
If the file is displayed in your browser you can use your browser's > File > Save as 
commands to save it, but make sure you use a .csv extension with no other extension added.

## Installation of python module
Use of Python permits further customization the output.
This repository contains four libraries: `stewi`,`chemicalmatcher`,`facilitymatcher` and `stewicombo`. If you have Python 3.x installed, 
pip can be called to install the downloaded package. 

If you've downloaded and unzipped the file, open the command line and type
>pip install -e `directory_of_unzipped_folder`

where `directory_of_unzipped_folder` is a directory like `C:/Users/username/standardizedinventories`
This will install the three python libraries.

You can test the installation by opening up a Python console and entering
>> import stewi

>> import chemicalmatcher

>> import facilitymatcher

>> import stewicombo

If no error code is returned, the libraries are installed.

## Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis 
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer 
has responsibility to protect the integrity , confidentiality, or availability of the information. 
Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, 
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  
The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity 
by EPA or the United States Government.
