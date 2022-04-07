"""Create and store inventories."""

import pytest

import stewi
from stewi.globals import config


requires_browser_download = {'RCRAInfo'}
SKIP_BROWSER_DOWNLOAD = True


@pytest.mark.inventory
def generate_inventories(year):
    for inventory in config()['databases']:
        if SKIP_BROWSER_DOWNLOAD and inventory in requires_browser_download:
            continue
        if inventory.isin(['DMR']):
            continue
        df = stewi.getInventory(inventory, year)


if __name__ == "__main__":
    year = 2017
    generate_inventories(year)
