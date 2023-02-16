import requests
from requests.exceptions import Timeout
import pandas as pd
import creds
import coins
import logging

"""""""""""""""""""""
******LOGGING*****
"""""""""""""""""""""

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

file_handler = logging.FileHandler('failures.log')
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

"""""""""""""""""""""
******START*****
"""""""""""""""""""""

crypto_coins = coins.crypto_coins

url = f'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'

headers = {
    'Accepts': 'application/json',
    'X-CMC_Pro_API_Key': creds.cap_api_key
}

parameters = {
    'convert': 'USD'
}


def extract() -> list:

    max_retries = 3

    market_cap = []

    for coin in crypto_coins:
        for i in range(max_retries):
            # Get data for the coin up top 3 times.
            try:
                parameters['symbol'] = coin
                response = requests.get(url, params=parameters, headers=headers)
                if response.status_code == 200:
                    response_json = response.json()
                    market_cap.append(response_json['data'])
                    break
                else:
                    logger.warning(f'Error code {response.status_code} in coinmarketcap for coin {coin}: {response.reason}')
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
        'volume_24h': [],
        'market_cap': []
    }

    for i in data:
        for key, value in i.items():
            for item in value:
                rows['name'].append(item['name'])
                rows['symbol'].append(item['symbol'])
                rows['volume_24h'].append(item['quote']['USD']['volume_24h'])
                rows['market_cap'].append(item['quote']['USD']['market_cap'])

    df = pd.DataFrame(rows)
    if df.isnull().values.any():
        logger.error('Null values were found in the dataframe for coinmarketcap api. Script terminated.')
        raise Exception('Null values were found.')
    elif df.empty:
        logger.error('Dataframe is empty. Script terminated.')
        raise Exception('df is empty.')
    else:
        return df


def transform() -> pd.DataFrame:
    df = make_df()

    # Convert number columns from string to float.
    number_columns = df[df.columns[2:]]
    df[number_columns.columns] = number_columns.apply(pd.to_numeric).round(2)

    return df


coin_market_cap_df = transform()
