import threading
from concurrent import futures
from datetime import datetime

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine


class StockInfoDownloader(object):
    """
    An object used to download the latest 11 years of daily stock values on
    S & P 500 Companies
    """

    def __init__(self, engine):
        """
        Initialize object variables
        """
        self.semaphore = threading.Semaphore(1)
        self.current_time = datetime.now()
        self.start_time = datetime(self.current_time.year - 11, self.current_time.month, self.current_time.day)
        self.stock_list = []
        self.engine = engine
        self.main_df = pd.DataFrame({})

    def download_stock(self, stock: str) -> pd.DataFrame:
        """
        Download the data regarding a specific stock from yfinance

        :param stock: String; The stock symbol (e.g. GOOG)
        :return: pd.Dataframe; a Dataframe containing the stock information gathered over the desired period
        """
        # Semaphore was implemented as yfinance restricts concurrent calls to it's api
        self.semaphore.acquire()
        print("Downloading data regarding stock: ", stock)
        stock_df = yf.download(stock, self.start_time, self.current_time)
        self.semaphore.release()

        # Add column distinguishing which company relates to the gathered data
        stock_df['Name'] = stock

        # Convert Date column from datetime to date
        stock_df.reset_index(inplace=True)
        stock_df['Date'] = pd.to_datetime(stock_df['Date']).dt.date

        return stock_df

    def begin_download(self) -> None:
        """
        Gathers the latest list of S & P Companies and then concurrently downloads the data of each company
        Uploads the data collected to an SQL table.

        :return: None
        """
        # Collect a list of the current S&P Companies from wikipedia
        wiki_s_and_p_table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')

        # Format the data collected to be usable in executor.map
        stock_list = wiki_s_and_p_table[0]['Symbol'].str.replace('.', '-', regex=False).to_list()

        # Concurrently collect the data from each of the S & P 500 companies
        with futures.ThreadPoolExecutor(10) as executor:
            df_list = []
            for res in executor.map(self.download_stock, stock_list):
                df_list.append(res)

        # Concatenate the collected dataframes into a single dataframe
        self.main_df = pd.concat(df_list, ignore_index=True)

        # Push the collected data to a specified sql table
        self.main_df.to_sql('s_and_p', con=self.engine, if_exists='replace')

        # Prints statistics regarding the download time
        time_elapsed = datetime.now() - self.current_time
        minutes, seconds = divmod(time_elapsed.seconds, 60)
        print(f'Download completed in {minutes} minutes and {seconds} seconds.')


if __name__ == '__main__':

    # Attempt to connect to postgres db
    try:
        engine = create_engine('postgresql://postgres:postgres@127.0.0.1:5432/historicalstockdb')
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)

    StockInfoDownloader(engine).begin_download()
