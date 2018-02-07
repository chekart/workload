from jobs import count_eur_currency_countries


if __name__ == '__main__':
    codes = []
    with open('countries.txt', 'r') as f:
        for line in f.readlines():
            codes.append(line.split()[0])

    count_eur_currency_countries.distribute(codes)
    count_eur_currency_countries.wait_results()

    print('{} countries use EUR currency'.format(len(list(count_eur_currency_countries.results))))
