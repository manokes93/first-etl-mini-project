import requests
from requests.exceptions import Timeout
import pandas as pd
import coins
import logging
import os

# Logging setup

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

file_handler = logging.FileHandler('failures.log')
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# ETL start

crypto_coins = coins.crypto_coins

cap_api_key = os.environ['cap_api_key']

url = f'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'

headers = {
    'Accepts': 'application/json',
    'X-CMC_Pro_API_Key': cap_api_key
}

parameters = {
    'convert': 'USD'
}


def extract() -> list:
    market_cap = []

    for coin in crypto_coins:
        for i in range(3):
            # Get data for the coin up top 3 times.
            try:
                parameters['symbol'] = coin
                response = requests.get(url, params=parameters, headers=headers)
                if response.status_code == 200:
                    response_json = response.json()
                    market_cap.append(response_json['data'])
                    break
                else:
                    logger.warning(f'Error code {response.status_code} in coinmarketcap '
                                   f'for coin {coin}: {response.reason}')
            except Timeout:
                logger.warning(f'Timeout error in coinmarketcap api call on coin: {coin}')
        else:
            logger.error('All retries failed.')
            raise Exception('Extract failed.')
    return market_cap


def make_df() -> pd.DataFrame:
    data = extract()

    # Set row lists
    rows = {
        'name': [],
        'symbol': [],
        'Volume': [],
        'Market_Cap': []
    }

    for i in data:
        for key, value in i.items():
            for item in value:
                rows['name'].append(item['name'])
                rows['symbol'].append(item['symbol'])
                rows['Volume'].append(item['quote']['USD']['volume_24h'])
                rows['Market_Cap'].append(item['quote']['USD']['market_cap'])

    df = pd.DataFrame(rows)
    return df


def transform() -> pd.DataFrame:
    df = make_df()

    # Convert number columns from string to float.
    number_columns = df[df.columns[2:]]
    df[number_columns.columns] = number_columns.astype(float).round(2)

    print('Created coin_market_cap dataframe')
    return df


coin_market_cap_df = transform()
