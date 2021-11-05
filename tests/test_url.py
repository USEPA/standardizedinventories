"""Test config file urls."""

from stewi.globals import url_is_alive


def test_stewi_config():
    from stewi.globals import config
    _config = config()['databases']
    url_list = []

    # RCRAInfo, TRI, DMR
    for inv in ['RCRAInfo', 'TRI', 'DMR']:
        url_list.append(_config[inv]['url'])

    # eGRID
    for k, v in _config['eGRID'].items():
        if isinstance(v, dict) and 'download_url' in v:
            url_list.append(v['download_url'])

    # GHGRP
    ghgrp = _config['GHGRP']
    url_list.extend([ghgrp['url'] + u for u in [ghgrp['lo_subparts_url'],
                                                ghgrp['esbb_subparts_url'],
                                                ghgrp['data_summaries_url']]])

    url_check = {}
    for url in url_list:
        if url not in url_check.keys():
            url_check[url] = url_is_alive(url)
    error_list = [k for k, v in url_check.items() if not v]
    s = '\n'.join(error_list)
    assert all(url_check.values()), f"error in {s}"


def test_facilitymatcher_config():
    from facilitymatcher.globals import FRS_config

    assert url_is_alive(FRS_config['url'])


if __name__ == "__main__":
    test_stewi_config()
    test_facilitymatcher_config()
