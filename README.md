# Standardized Emission and Waste Inventories (StEWI)

StEWI is a collection of Python modules that provide processed USEPA emission and waste generation inventory data in standard tabular formats.
 The standard outputs may be further aggregated or filtered based on given criteria, and can be combined based on common facility and flows
  across the inventories.

StEWI consists of a core module, `stewi`, that digests and provides the USEPA inventory data in standard formats. Two matcher modules, the `facilitymatcher`
and `chemicalmatcher`, provide commons IDs for facilities and flows across inventories, which is used by the `stewicombo` module
to combine the data, and optionally remove overlaps and remove double counting of groups of chemicals based on user preferences.

## USEPA Inventories Covered By Data Reporting Year (current version)

|Source|2001|2002|2003|2004|2005|2006|2007|2008|2009|2010|2011|2012|2013|2014|2015|2016|2017|2018|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|[Toxic Release Inventory](https://www.epa.gov/toxics-release-inventory-tri-program)*|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|
|[RCRA Biennial Report](https://www.epa.gov/hwgenerators/biennial-hazardous-waste-report)|x| |x| |x| |x| |x| |x| |x| |x| |x| |
|[National Emissions Inventory](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei)**| | | | | | | | | | |x| | |x| |x|x| |
|[Emissions & Generation Resource Integrated Database](https://www.epa.gov/energy/emissions-generation-resource-integrated-database-egrid)| | | | | | | | | | | | | |x| |x| |x|
|[Discharge Monitoring Reports](https://www.epa.gov/)| | | | | | | | | | | | | |x|x|x|x| |
|[Greenhouse Gas Reporting Program](https://www.epa.gov/ghgreporting)| | | | | | | | | | | | |x |x |x |x |x |x |

*TRI available back through 1988
**Only point sources included at this time from NEI

## Standard output formats

The core `stewi` module produces the following output formats:

[Flow-By-Facility](./format%20specs/FlowByFacility.md): Each row represents the total amount of release or waste of a single type in a given year from the given facility.

[Facility](./format%20specs/Facility.md): Each row represents a unique facility in a given inventory and given year

[Flow](./format%20specs/Flow.md):  Each row represents a unique flow (substance or waste) in a given inventory and given year

The `chemicalmatcher` module produces:

[Chemical Matches](./format%20specs/ChemicalMatches.md): Each row provides a common identifier for an inventory flow chemical

The `facilitymatcher` module produces:

[Facility Matches](./format%20specs/FacilityMatches.md): Each row provides a common identifier for an inventory facility

The `stewicombo` module produces:

[Flow-By-Facility Combined](./format%20specs/FlowByFacilityCombo.md): Analagous to the flowbyfacility, with chemical and facilitymatches added

## Data Processing

The following describes details related to the dataset processing specific to each dataset

### DMR

Processing of the DMR uses the custom search option of the [Water Pollutant Loading Tool](https://echo.epa.gov/trends/loading-tool/get-data/custom-search/) with the following parameters:
- Parameter grouping: On - applies a parameter grouping function to avoid double-counting loads for pollutant parameters that represent the same pollutant
- Detection limit: Half - set all non-detects to ½ the detection limit
- Estimation: On - estimates loads when monitoring data are not reported for one or more monitoring periods in a reporting year
- Nutrient Aggregation: On - Nitrogen and Phosphorous flows are converted to N and P equivalents

## Wiki

See the [Wiki](https://github.com/USEPA/standardizedinventories/wiki) for instructions on installation and use and for
contact information.

## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer
has responsibility to protect the integrity , confidentiality, or availability of the information.
Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  
The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity
by EPA or the United States Government.
