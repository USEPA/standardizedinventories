"""Test the creation of all inventories."""

import stewi
from stewi.globals import config

year = 2018


def test_inventory_generation():
    error_list = []
    for inventory in config()['databases']:
        # skip RCRAInfo due to browswer download, DMR due to time constraints
        if inventory in ['RCRAInfo', 'DMR', 'GHGRP']:
            continue
        df = stewi.getInventory(inventory, year)

        error = df is None
        if not error:
            error = len(df) == 0
        if error:
            error_list.append(inventory)

    assert len(error_list) == 0, f"Generation of {','.join(error_list)} unsuccessful"


if __name__ == "__main__":
    test_inventory_generation()
