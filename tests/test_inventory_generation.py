"""Test the creation of all inventories."""

import pytest

import stewi
from stewi.globals import config, generate_inventory
from stewi.exceptions import InventoryNotAvailableError
import stewicombo
import facilitymatcher


year = 2018

@pytest.mark.skip(reason="perform separate inventory tests")
def test_all_inventory_generation():
    error_list = []
    for inventory in config()['databases']:
        df = stewi.getInventory(inventory, year)
        error = df is None or len(df) == 0
        if error:
            error_list.append(inventory)
    assert len(error_list) == 0, f"Generation of {','.join(error_list)} unsuccessful"


@pytest.mark.parametrize("year", [2020])
@pytest.mark.inventory
def test_generate_inventories(year):
    for inventory in config()['databases']:
        try:
            generate_inventory(inventory, year)
        except InventoryNotAvailableError as err:
            print(err)
            continue


@pytest.mark.combined
def test_generate_fm_files():
    df_naics = facilitymatcher.get_FRS_NAICSInfo_for_facility_list(
                    frs_id_list=None, inventories_of_interest_list=None,
                    download_if_missing=False)
    df_facilities = facilitymatcher.get_matches_for_inventories()
    assert df_naics is not None and df_facilities is not None


@pytest.mark.parametrize("name,compartment,inv_dict",
                         [("NEI_TRI_air_seccntx_2017", "air", {"NEI":"2017", "TRI":"2017"}),
                          ("TRI_DMR_2017", "water", {"TRI":"2017", "DMR":"2017"}),
                          ("TRI_GRDREL_2017", "soil", {"TRI":"2017"})])
@pytest.mark.combined
def test_generate_combined_inventories(name, compartment, inv_dict):
    keep_sec_cntx = True if compartment == 'air' else False
    df = stewicombo.combineFullInventories(inv_dict, filter_for_LCI=True,
                                           remove_overlap=True,
                                           compartments=[compartment],
                                           keep_sec_cntx=keep_sec_cntx,
                                           download_if_missing=True)
    stewicombo.saveInventory(name, df, inv_dict)
    df2 = stewicombo.getInventory(name, download_if_missing=False)
    assert df2 is not None


def test_NEI_generation():
    assert stewi.getInventory('NEI', year) is not None


def test_TRI_generation():
    assert stewi.getInventory('TRI', year) is not None


def test_eGRID_generation():
    assert stewi.getInventory('eGRID', year) is not None


def test_GHGRP_generation():
    assert stewi.getInventory('GHGRP', year) is not None


@pytest.mark.skip(reason="DMR is skipped for time constraints")
def test_DMR_generation():
    assert stewi.getInventory('DMR', year) is not None


def test_RCRAInfo_generation():
    assert stewi.getInventory(
        'RCRAInfo', year if year % 2 == 1 else year + 1) is not None


def test_existing_inventories():
    assert stewi.getAvailableInventoriesandYears() is not None


if __name__ == "__main__":
    # test_all_inventory_generation()
    # test_generate_inventories(2017)
    # test_generate_fm_files()
    test_generate_combined_inventories("TRI_DMR_2017",
                                       "water",
                                       {"TRI":"2017", "DMR":"2017"})
