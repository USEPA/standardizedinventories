"""Test the creation of all inventories."""

import pytest

import stewi
from stewi.globals import config


year = 2018
requires_browser_download = {'RCRAInfo'}
SKIP_BROWSER_DOWNLOAD = True


@pytest.mark.skip(reason="perform separate inventory tests")
def test_all_inventory_generation():
    error_list = []

    for inventory in config()['databases']:
        if SKIP_BROWSER_DOWNLOAD and inventory in requires_browser_download:
            continue

        df = stewi.getInventory(inventory, year)

        error = df is None or len(df) == 0
        if error:
            error_list.append(inventory)

    assert len(error_list) == 0, f"Generation of {','.join(error_list)} unsuccessful"


def test_NEI_generation():
    assert stewi.getInventory('NEI', year) is not None


def test_TRI_generation():
    assert stewi.getInventory('TRI', year) is not None


@pytest.mark.skip(reason="GHGRP is skipped for time constraints")
def test_GHGRP_generation():
    assert stewi.getInventory('GHGRP', year) is not None


def test_eGRID_generation():
    assert stewi.getInventory('eGRID', year) is not None


@pytest.mark.skip(reason="DMR is skipped for time constraints")
def test_DMR_generation():
    assert stewi.getInventory('DMR', year) is not None


@pytest.mark.skip(reason="RCRAInfo requires browser download")
def test_RCRAInfo_generation():
    assert stewi.getInventory('RCRAInfo', year) is not None


def test_existing_inventories():
    assert stewi.getAvailableInventoriesandYears() is not None


if __name__ == "__main__":
    test_all_inventory_generation()
