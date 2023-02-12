import requests
import pandas as pd
import creds
import coins

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
    market_cap = []

    for coin in crypto_coins:
        parameters['symbol'] = coin
        response = requests.get(url, params=parameters, headers=headers).json()
        market_cap.append(response['data'])
    return market_cap


def make_df():
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
    return df


make_df()
