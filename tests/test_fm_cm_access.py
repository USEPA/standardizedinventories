"""Test the access of facilitymatcher and chemicalmatcher datasets."""

import pytest

from stewi.globals import config
import chemicalmatcher
import facilitymatcher


@pytest.mark.skip(reason="skip download of facility matcher source data "
                  "due to file size")
def test_facilitymatcher_file():
    assert facilitymatcher.get_matches_for_inventories(["TRI"]) is not None


def test_chemical_matches():
    assert chemicalmatcher.get_matches_for_StEWI(
        config()['databases'].keys()) is not None


if __name__ == "__main__":
    test_chemical_matches()
    test_facilitymatcher_file()
