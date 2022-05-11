"""Generate inventory files via command line"""

import stewi


def main(inventory, years):
    if '-' in years:
        years_list = years.split('-')
        year_iter = list(range(int(years_list[0]), int(years_list[1]) + 1))
    else:
        year_iter = [years]
    for i_year in year_iter:
        stewi.globals.generate_inventory(inventory, i_year)

if __name__=="__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('--years', help='singel year or years separated'
                        'by dash')
    parser.add_argument('--inventory', help='inventory acroynym')

    args = vars(parser.parse_args())

    main(args['inventory'], args['years'])
