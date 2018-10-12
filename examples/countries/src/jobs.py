import os
import redis
import requests
import logging

from workload import distributed

logging.getLogger('distributed').setLevel(logging.DEBUG)
logging.getLogger('distributed').addHandler(logging.StreamHandler())

REDIS_POOL = redis.ConnectionPool(host=os.getenv('REDIS_HOST'))


@distributed('count_eur_currency_countries', redis_pool=REDIS_POOL)
def count_eur_currency_countries(job, country):
    response = requests.get('https://restcountries.eu/rest/v2/alpha/{}'.format(country), {
        'fullText': 'true',
    })
    content = response.json()

    name = content['name']
    if 'eur' in (currency['code'].lower() for currency in content['currencies'] if currency['code']):
        job.result(name)
