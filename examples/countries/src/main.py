import time
from jobs import count_eur_currency_countries


if __name__ == '__main__':
    codes = []
    with open('countries.txt', 'r') as f:
        for line in f.readlines():
            codes.append(line.split()[0])

    start_time = time.time()
    count_eur_currency_countries.distribute(codes)
    count_eur_currency_countries.wait_results()

    countries = map(
        lambda country: country.decode('utf-8'),
        count_eur_currency_countries.results)
    countries = sorted(countries)

    print('Work completed in {:.2f} s'.format(time.time() - start_time))
    print('{} countries use EUR currency:'.format(len(countries)))
    print(', '.join(countries))
