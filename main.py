import requests
import pandas as pd
import creds
import coins
from datetime import date, timedelta
import uuid

crypto_coins = coins.crypto_coins
today = date.today()
yesterday = today - timedelta(days=1)


def extract() -> list:
    # Extracts data from the API.
    # This response does not return the coin and currency, so the coin gets added into the exchange_rates list object.
    # The currency gets taken care of during the make_df function. This is because I only care about USD.
    # The coin api only allows 100 requests daily in the free tier.

    exchange_rates = []
    for coin in crypto_coins:
        # This is the url to use to get historical data.
        # This has 1DAY already as the periodicity argument.
        # The output returns midnight of yesterday to 11:59pm of that same day.
        url = f'https://rest.coinapi.io/v1/exchangerate/{coin}/USD/history?period_id=1DAY&time_start={yesterday}T00:00:00&time_end={today}T00:00:00'
        headers = {'X-CoinAPI-Key': creds.api_key}
        response = requests.get(url, headers=headers)
        exchange_rates.append(coin)
        exchange_rates.append(response.json())
    return exchange_rates


def make_df() -> pd.DataFrame:
    # Creates a pandas dataframe
    # Not including the following columns: time_open, time_close
    data = extract()

    # Set row lists
    rows = {
        'asset_id_base': [],
        'asset_id_quote': [],
        'time_period_start': [],
        'time_period_end': [],
        'rate_open': [],
        'rate_high': [],
        'rate_low': [],
        'rate_close': []
    }

    # Add data to lists
    for i in data:
        if type(i) == str:
            rows['asset_id_base'].append(i)
            rows['asset_id_quote'].append('USD')
        elif type(i) == list:
            rows['time_period_start'].append(i[0]["time_period_start"])
            rows['time_period_end'].append(i[0]["time_period_end"])
            rows['rate_open'].append(i[0]["rate_open"])
            rows['rate_high'].append(i[0]["rate_high"])
            rows['rate_low'].append(i[0]["rate_low"])
            rows['rate_close'].append(i[0]["rate_close"])
        else:
            print('Error in dataframe creation: Invalid data type in extract.')

    df = pd.DataFrame(rows)
    return df


def transform() -> pd.DataFrame:
    df = make_df()
    # Convert number columns from string to float.
    number_columns = df[df.columns[4:]]
    df[number_columns.columns] = number_columns.apply(pd.to_numeric).round(2)

    # Convert time_period columns from string to date
    date_columns = df[df.columns[2:4]]
    df[date_columns.columns] = date_columns.apply(pd.to_datetime)

    # Add unique row id
    df['uuid'] = [str(uuid.uuid4()) for i in range(len(df))]

    # Reorder columns so uuid comes first
    reordered_df = df.iloc[:, [8, 0, 1, 2, 3, 4, 5, 6, 7]]

    return reordered_df

# def load():
#     df = transform()
#
#     df.to_gbq(
#         destination_table="",
#         project_id="",
#         table_schema=,
#         credentials=""
#     )


if __name__ == '__main__':
    print(transform())
    print(transform().dtypes)

"""
Stuff to add:
-Make a log file
-If any values are null, fail it.
-If the crypto ticker is not found, continue, but print an error.
-if connection fails, automatically retry a few times.
-if dataframe is empty, make it fail.
-if the time_period_start and time_period_end is not yesterday, fail it.
"""
