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
    'symbol': 'BTC',
    'convert': 'USD'
}

response = requests.get(url, params=parameters, headers=headers)

data = response.json()

print(data)