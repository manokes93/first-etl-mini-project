import requests
import snowflake.connector
from requests.exceptions import Timeout
import pandas as pd
import creds
import coins
from datetime import date, timedelta
import uuid
import coin_market_cap as cmc
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.errors import OperationalError, ProgrammingError

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
        url = f'https://rest.coinapi.io/v1/exchangerate/{coin}/USD/history?period_id=1DAY&time_start={yesterday}' \
              f'T00:00:00&time_end={today}T00:00:00'
        headers = {'X-CoinAPI-Key': creds.api_key}
        for i in range(3):
            # Get data for the coin up top 3 times.
            try:
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    exchange_rates.append(coin)
                    exchange_rates.append(response.json())
                    break
                else:
                    logger.warning(f'Error code {response.status_code} in coin api for coin {coin}: {response.reason}')
            except Timeout:
                logger.warning(f'Timeout error in coin api call on coin: {coin}')
        else:
            logger.error('All retries failed.')
            raise Exception('Extract failed.')
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
            rows['rate_open'].append(i[0]["rate_open"])
            rows['rate_high'].append(i[0]["rate_high"])
            rows['rate_low'].append(i[0]["rate_low"])
            rows['rate_close'].append(i[0]["rate_close"])
        else:
            logger.error('Error in dataframe creation: Invalid data type in extract.')
            raise Exception('Invalid data type in extract.')

    df = pd.DataFrame(rows)

    # Check dataframe for nulls and fail the pull if any nulls are found.
    if df.isnull().values.any():
        logger.error('Null values were found in the dataframe for coin api. Script terminated.')
        raise Exception('Null values were found.')
    elif df.empty:
        logger.error('Dataframe is empty. Script terminated.')
        raise Exception('df is empty.')
    else:
        return df


def transform() -> pd.DataFrame:
    df = make_df()
    # Convert number columns from string to float.
    number_columns = df[df.columns[3:]]
    df[number_columns.columns] = number_columns.astype(float).round(2)

    # Convert time_period columns from string to date
    df['Date'] = pd.to_datetime(df['time_period_start'], format='%Y-%m-%d').dt.date

    # Drop time_period_start
    df.drop(columns=['time_period_start'], inplace=True)

    # Add unique row id
    df['uuid'] = [str(uuid.uuid4()) for _ in range(len(df))]

    print('Created coinapi dataframe.')
    return df


def join():
    right_df = cmc.coin_market_cap_df
    left_df = transform()

    # Join data in from coin_market_cap
    joined_df = left_df.merge(right_df.rename({'symbol': 'asset_id_base'}, axis=1), on='asset_id_base', how='left')

    # Reorder columns so uuid comes first, and name comes after the symbol
    reordered_df = joined_df.reindex(columns=[
        'uuid',
        'asset_id_base',
        'name',
        'asset_id_quote',
        'Date',
        'rate_open',
        'rate_high',
        'rate_low',
        'rate_close',
        'Volume',
        'Market_Cap'
    ])

    print('Both dataframes are joined.')
    return reordered_df


def load():

    # Call the dataframe
    df = join()

    # Drop the index and make columns uppercase
    df.reset_index(drop=True, inplace=True)
    df.columns = df.columns.str.upper()

    # Establish snowflake connection
    cnn = snowflake.connector.connect(
        user=creds.snow_user,
        password=creds.snow_password,
        account=creds.snow_account,
        warehouse=creds.snow_warehouse,
        database=creds.snow_database,
        schema=creds.snow_schema
    )

    try:
        success, nchunks, nrows, output = write_pandas(cnn, df, 'OHLCV')
        print(str(success) + ', ' + str(nchunks) + ', ' + str(nrows))
        print('Upload to snowflake done.')
    except OperationalError as e:
        logger.error(f'Snowflake connection failed because of operational error: {e}')
    except ProgrammingError as e:
        print(f'Snowflake connection failed because of programming error: {e}')
    except Exception as e:
        print(f'Snowflake connection failed because of unexpected error: {e}')
    finally:
        cnn.close()


if __name__ == '__main__':
    load()
