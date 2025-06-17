# StEWI ‒ **User Guide**

This guide expands on the top-level `README.md` and focuses on **how to interact with the library from Python code**.  It documents the public functions that are automatically available once you import the core packages and illustrates typical usage patterns.

---

## What is an "Inventory" in StEWI?

In the context of StEWI, an **inventory** is a specific, structured dataset published by the U.S. Environmental Protection Agency (EPA) that reports facility-level emissions or waste generation for a given year. Each inventory is a distinct EPA program or database, with its own data collection methods, reporting requirements, and scope.

**Examples of inventories in StEWI:**
- **TRI**: Toxic Release Inventory
- **NEI**: National Emissions Inventory
- **DMR**: Discharge Monitoring Report
- **GHGRP**: Greenhouse Gas Reporting Program
- **eGRID**: Emissions & Generation Resource Integrated Database
- **RCRAInfo**: Resource Conservation and Recovery Act Biennial Report

**Key points:**
- Each inventory covers a different aspect of environmental reporting (e.g., air emissions, water discharges, hazardous waste).
- Inventories are identified by their acronym (e.g., 'TRI', 'NEI') and a reporting year (e.g., 2021).
- StEWI standardizes the data from these inventories into common tabular formats, making it easier to analyze, compare, and combine data across programs.

**In code:**
When you see a function parameter like `inventory_acronym` or `inventory_dict`, it refers to one or more of these EPA datasets, not a generic Python data structure.

**Example usage:**
```python
# Get 2021 data from the TRI inventory
tri_2021 = stewi.getInventory('TRI', 2021)
```

**Summary:**
An "inventory" in StEWI is a specific EPA dataset of facility-level environmental data for a given year, such as TRI, NEI, DMR, etc.

---

## 1. Installation & First Import
(See the *Installation Instructions* section in `README.md` for full details.)

```bash
pip install git+https://github.com/USEPA/standardizedinventories.git@v1.1.0
```

Then, in Python:

```python
import stewi                                # core inventory access
import facilitymatcher                     # facility cross-walks
import chemicalmatcher                     # chemical cross-walks
import stewicombo                          # multi-inventory combination helpers
```

All examples below assume these imports.

---

## 2. Quick-start Example
```python
# 1. Discover what is available
stewi.printAvailableInventories('flowbyfacility')

# 2. Download or generate a Flow-By-Facility inventory
tri_2021 = stewi.getInventory('TRI', 2021, stewiformat='flowbyfacility')
print(len(tri_2021), 'rows downloaded')

# 3. Peek at facility-level information only
facilities = stewi.getInventoryFacilities('TRI', 2021)

# 4. Combine TRI and NEI for 2021, removing overlap
combo = stewicombo.combineFullInventories({'TRI': '2021', 'NEI': '2021'})
print(combo.head())
```

---

## 3. Public API Reference
Below is an at-a-glance table of the functions exposed at the top level of each major package, followed by more detailed explanations.

| Package | Function | Description (excerpt) |
|---------|----------|-----------------------|
| **stewi** | `getAllInventoriesandYears(year=None)` | Return every inventory and the list of data years it supports.  If `year` is provided, only the most recent year ≤ `year` is returned for each inventory. |
| | `getAvailableInventoriesandYears(stewiformat='flowbyfacility')` | List inventories/years already *present locally* for the given output format. |
| | `printAvailableInventories(stewiformat='flowbyfacility')` | Convenience printer around the previous function. |
| | `getInventory(acronym, year, stewiformat='flowbyfacility', filters=None, download_if_missing=False, …)` | Generate (or load, if cached) an inventory in the requested standard format. |
| | `getInventoryFlows(acronym, year)` | Load only the Flow table for the given inventory/year. |
| | `getInventoryFacilities(acronym, year)` | Load only the Facility table for the given inventory/year. |
| | `getMetadata(acronym, year)` | Dictionary of provenance metadata captured at generation time. |
| | `seeAvailableInventoryFilters()` | Print the names & descriptions of predefined filters that can be passed to `getInventory`. |
| **facilitymatcher** | `get_matches_for_inventories(inventory_list)` | Retrieve facility cross-walks (FRS IDs) for the given inventories. |
| | `get_FRS_NAICSInfo_for_facility_list(frs_ids, inventories_of_interest)` | Return NAICS codes for specific FRS IDs. |
| | `get_matches_for_id_list(base_inventory, id_list, inventory_list)` | Facility matches starting from a list of IDs in a base inventory. |
| **chemicalmatcher** | `get_matches_for_StEWI(inventory_list=None)` | Retrieve chemical cross-walks (SRS IDs) for the supplied inventories. |
| | `get_program_synomyms_for_CAS_list(cas_list, inventories)` | Fetch program-specific chemical names via the EPA SRS web service. |
| **stewicombo** | `combineFullInventories(inventory_dict, remove_overlap=True, …)` | Combine multiple full inventories into one Flow-By-Facility dataset. |
| | `combineInventoriesforFacilitiesinBaseInventory(base_inv, inventory_dict, …)` | Combine inventories but only for facilities present in *base_inv*. |
| | `combineInventoriesforFacilityList(base_inv, inventory_dict, facility_id_list, …)` | Combine inventories for an explicit list of facilities. |
| | `saveInventory(name, df, inventory_dict)` | Persist a combined inventory (with metadata) locally. |
| | `getInventory(name, download_if_missing=False)` | Retrieve a previously saved combined inventory by name or filename. |
| | `pivotCombinedInventories(df)` | Convenience pivot table (rows: facility/SRS/compartment, columns: Source). |

> **Tip** Because each helper function returns a *pandas* `DataFrame`, you have the full analytical power of the pandas ecosystem for further processing.

---

## 4. Detailed Function Signatures & Examples
### 4.1 `stewi.getInventory`
```python
stewi.getInventory(
    inventory_acronym: str,   # e.g. 'TRI', 'NEI', 'DMR', …
    year: int,                # data year
    stewiformat: str = 'flowbyfacility',
    filters: list[str] | None = None,
    download_if_missing: bool = False,
    keep_sec_cntx: bool = False,
    # legacy params retained for backward-compatibility
    filter_for_LCI: bool = False,
    US_States_Only: bool = False,
) -> pandas.DataFrame
```

Example with filters:
```python
inv = stewi.getInventory('NEI', 2017,
                         filters=['filter_for_LCI', 'US_States_only'],
                         download_if_missing=True)
```

Available filter names can be inspected with `stewi.seeAvailableInventoryFilters()`.

### 4.2 `stewicombo.combineFullInventories`
```python
combo = stewicombo.combineFullInventories(
    {'TRI': '2021', 'NEI': '2021'},
    remove_overlap=True,
    compartments=['air', 'water']
)
```
The resulting DataFrame is in *Flow-By-Facility Combined* format (see *format specs* folder) and already contains matched facility (FRS_ID) and chemical (SRS_ID) identifiers.

### 4.3 Retrieving Cross-walks Directly
```python
frs_matches = facilitymatcher.get_matches_for_inventories(['TRI', 'NEI'])
cas_synonyms = chemicalmatcher.get_program_synomyms_for_CAS_list(
    ['124-38-9', '74-82-8'], ['TRI']
)
```

---

## 5. Working with Secondary Contexts
By default, many helper functions collapse secondary contexts (e.g., `air/stack`) to their primary compartment (`air`).  Set `keep_sec_cntx=True` in `stewi.getInventory` or in the `stewicombo.combine*` functions to preserve full context strings.

---

## 6. Troubleshooting & Advanced Topics
• **Missing files** – Set `download_if_missing=True` to automatically pull pristine data from the EPA-hosted S3 bucket.

• **Metadata** – Use `stewi.getMetadata('TRI', 2021)` to inspect source URLs, hashes, and software version for a particular inventory.

• **Performance** – Large inventories can be memory-intensive.  Consider filtering early and writing intermediate results to `parquet` if you only need a subset.

---

## 7. Additional Resources
* **Data format definitions** – See the markdown files in the `format specs/` directory for the column-level specification of every standardized dataset.
* **StEWI publication** – Cite *Applied Sciences 12(7)*: *https://doi.org/10.3390/app12073447*.
* **EPA Documentation** – Each source inventory (TRI, NEI, …) has its own primary documentation on *epa.gov*; links are provided in `README.md`.

---

*Last updated: {{DATE}} – Generated automatically via pull request.* 