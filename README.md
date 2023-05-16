# Standardized Emission and Waste Inventories (StEWI)
[![DOI - 10.3390/app12073447](https://img.shields.io/badge/DOI-10.3390%2Fapp12073447-blue)](https://doi.org/10.3390/app12073447)
[![DOI - 10.23719/1526441](https://img.shields.io/badge/v1.0%20DataProducts-10.23719%2F1526441-blue)](https://doi.org/10.23719/1526441)
[![build](https://github.com/USEPA/standardizedinventories/actions/workflows/python-package.yml/badge.svg)](https://github.com/USEPA/standardizedinventories/actions/workflows/python-package.yml)

StEWI is a collection of Python modules that provide processed USEPA facility-based emission and waste generation inventory data in standard tabular formats.
 The standard outputs may be further aggregated or filtered based on given criteria, and can be combined based on common facility and flows
  across the inventories.

StEWI consists of a core module, `stewi`, that digests and provides the USEPA inventory data in standard formats. Two matcher modules, the `facilitymatcher`
and `chemicalmatcher`, provide commons IDs for facilities and flows across inventories, which is used by the `stewicombo` module
to combine the data, and optionally remove overlaps and remove double counting of groups of chemicals based on user preferences.

StEWI v1 was peer-reviewed internally at USEPA and externally through _Applied Sciences_. An article describing StEWI was published in a special issue of Applied Sciences: [Advanced Data Engineering for Life Cycle Applications](https://doi.org/10.3390/app12073447).

## USEPA Inventories Covered By Data Reporting Year (current version)

|Source|2011|2012|2013|2014|2015|2016|2017|2018|2019|2020|2021|
|---|---|---|---|---|---|---|---|---|---|---|---|
|[Discharge Monitoring Reports](https://echo.epa.gov/tools/data-downloads/icis-npdes-dmr-and-limit-data-set)* | | | |x|x|x|x|x|x|x|x|
|[Greenhouse Gas Reporting Program](https://www.epa.gov/ghgreporting) |x|x|x|x|x|x|x|x|x|x|x|
|[Emissions & Generation Resource Integrated Database](https://www.epa.gov/energy/emissions-generation-resource-integrated-database-egrid) | | | |x| |x| |x|x|x|x|
|[National Emissions Inventory](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei)** |x|i|i|x|i|i|x|i|i|x| |
|[RCRA Biennial Report](https://www.epa.gov/hwgenerators/biennial-hazardous-waste-report)* |x| |x| |x| |x| |x| | |
|[Toxic Release Inventory](https://www.epa.gov/toxics-release-inventory-tri-program)* |x|x|x|x|x|x|x|x|x|x|x|

*Earlier data exist and are accessible but have not been validated

**Only point sources included at this time from NEI; _i_ interim years between triennial releases, accessed through the Emissions Inventory System, are not validated

## Standard output formats

The core `stewi` module produces the following output formats:

[Flow-By-Facility](./format%20specs/FlowByFacility.md): Each row represents the total amount of release or waste of a single type in a given year from the given facility.

[Flow-By-Process](./format%20specs/FlowByProcess.md): Each row represents the total amount of release or waste of a single type in a given year from a specific process within the given facility. Applicable only to NEI and GHGRP.

[Facility](./format%20specs/Facility.md): Each row represents a unique facility in a given inventory and given year

[Flow](./format%20specs/Flow.md): Each row represents a unique flow (substance or waste) in a given inventory and given year

The `chemicalmatcher` module produces:

[Chemical Matches](./format%20specs/ChemicalMatches.md): Each row provides a common identifier for an inventory flow chemical

The `facilitymatcher` module produces:

[Facility Matches](./format%20specs/FacilityMatches.md): Each row provides a common identifier for an inventory facility

The `stewicombo` module produces:

[Flow-By-Facility Combined](./format%20specs/FlowByFacilityCombo.md): Analagous to the flowbyfacility, with chemical and facilitymatches added

## Data Processing

The following describes details related to dataset access, processing, and validation

### DMR

Processing of the DMR uses the custom search option of the [Water Pollutant Loading Tool](https://echo.epa.gov/trends/loading-tool/get-data/custom-search/) with the following parameters:
- Parameter grouping: On - applies a parameter grouping function to avoid double-counting loads for pollutant parameters that represent the same pollutant
- Detection limit: Half - set all non-detects to ½ the detection limit
- Estimation: On - estimates loads when monitoring data are not reported for one or more monitoring periods in a reporting year
- Nutrient Aggregation: On - Nitrogen and Phosphorous flows are converted to N and P equivalents

For validation, the sum of facility releases (excluding N & P) are compared against reported state totals. Some validation issues are expected due to differences in default parameters used by the water pollutant loading tool for calculating state totals.

### eGRID

eGRID data are sourced from EPA's [eGRID](https://www.epa.gov/egrid) site.
For validation, the sum of facility releases are compared against reported U.S. totals by flow.

### GHGRP

GHGRP data are sourced from EPA's [Envirofacts API](https://enviro.epa.gov/)
For validation, the sum of facility releases by subpart are compared against reported U.S. totals by subpart and flow. The validation of some flows (HFC, HFE, and PFCs) are reported in carbon dioxide equivalents. Mixed reporting of these flows in the source data in units of mass or carbon dioxide equivalents results in validation issues.

### NEI

NEI data are downloaded from the EPA Emissions Inventory System (EIS) Gateway and hosted on EPA [Data Commons](https://edap-ord-data-commons.s3.amazonaws.com/index.html?prefix=stewi/) for access by StEWI.
For validation, the sum of facility releases are compared against reported totals by flow. Validation is only available for triennial datasets.

### RCRAInfo

RCRAInfo data are sourced from the [Public Data Files](https://rcrapublic.epa.gov/rcrainfoweb/action/main-menu/view)
For validation, the sum of facility waste generation are compared against reported state totals as calculated for the National Biennial Report.

### TRI

TRI data are sourced from the [Basic Plus Data files](https://www.epa.gov/toxics-release-inventory-tri-program/tri-data-and-tools)
For validation, the sum of facility releases are compared to national totals by flow from the TRI Explorer.

## Combined Inventories

`stewicombo` module combines inventory data from within and across selected inventories by matching facilities in the [Facility Registry Service](https://www.epa.gov/frs) and chemical flows using the [Substance Registry Service](https://sor.epa.gov/sor_internet/registry/substreg/LandingPage.do).
If the `remove_overlap` parameter is set to True (default), `stewicombo` combines records using the following default logic:
- Records that share a common compartment, SRS ID and FRS ID _within_ an inventory are summed.
- Records that share a common compartment, SRS ID and FRS ID _across_ an inventory are assessed by compartment preference (see `INVENTORY_PREFERENCE_BY_COMPARTMENT`).
- Additional steps are taken to avoid overlap of:
    - nutrient flow releases to water between the TRI and DMR
    - particulate matter releases to air reflecting PM < 10 and PM < 2.5 in the NEI
    - [Volatile Organic Compound (VOC)](https://github.com/USEPA/standardizedinventories/blob/master/stewicombo/data/VOC_SRS_IDs.csv) releases to air for individually reported VOCs and grouped VOCs


## Installation Instructions

Install a release directly from github using pip. From a command line interface, run:
> pip install git+https://github.com/USEPA/standardizedinventories.git@v1.0.5#egg=StEWI

where you can replace 'v1.0.5' with the version you wish to use under [Releases](https://github.com/USEPA/standardizedinventories/releases).

Alternatively, to install from the most current point on the repository:
```
git clone https://github.com/USEPA/standardizedinventories.git
cd standardizedinventories
pip install . # or pip install -e . for devs
```
The current version contains optional dependencies (`selenium` and `webdriver_manager`) to download RCRAInfo data using a chrome browswer interface prior to generating those stewi inventories.
See details in [RCRAInfo.py](https://github.com/USEPA/standardizedinventories/blob/master/stewi/RCRAInfo.py) for how to generate those inventories without these optional libraries.

To download these optional dependencies use one of the following pip install commands:

```
pip install .["RCRAInfo"]
```

or

```
pip install . -r requirements.txt -r rcrainfo_requirements.txt
```

### Secondary Context Installation Steps
In order to enable calculation and assignment of urban/rural secondary contexts, please refer to [esupy's README.md](https://github.com/USEPA/esupy/tree/main#installation-instructions-for-optional-geospatial-packages) for installation instructions, which may require a copy of the [`env_sec_ctxt.yaml`](https://github.com/USEPA/standardizedinventories/blob/master/env_sec_ctxt.yaml) file included here.

## Data Products
Output of StEWI can be accessed for selected releases without having to run StEWI. See the [Data Product Links](https://github.com/USEPA/standardizedinventories/wiki/DataProductLinks) page for direct links to StEWI output files in Apache parquet format.

## Wiki
See the [Wiki](https://github.com/USEPA/standardizedinventories/wiki) for instructions on installation and use and for
citation and contact information.

## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer
has responsibility to protect the integrity , confidentiality, or availability of the information.
Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.
The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity
by EPA or the United States Government.
