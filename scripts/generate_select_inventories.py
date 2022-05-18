"""Generate inventory files via command line"""

import stewi


def main(inventory, years):
    if '-' in years:
        years_list = years.split('-')
        year_iter = list(range(int(years_list[0]), int(years_list[1]) + 1))
    else:
        year_iter = [years]
    for i_year in year_iter:
        try:
            stewi.globals.generate_inventory(inventory, i_year)
        except stewi.exceptions.InventoryNotAvailableError as err:
            print(err)
            continue


if __name__=="__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('--years', help='single year or years separated'
                        'by dash')
    parser.add_argument('--inventory', help='inventory acroynym')

    args = vars(parser.parse_args())

    main(args['inventory'], args['years'])
