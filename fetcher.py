from datetime import datetime
import json	
import sys
		
import requests
import pandas as pd	
import numpy as np

import Quandl as q


COINBASE_ENDPOINT = 'https://www.coinbase.com/charts/price_history?days=365'
COINBASE_ENDPOINT_VARIABLE_DAYS = 'https://www.coinbase.com/charts/price_history?days=%s'

QUANDL_ENDPOINT = 'https://www.quandl.com/api/v1/datasets/%s/%s?sort_order=json.asc'

QUANDL_API_KEY = 'XL6P_9uvtZeqd6fFBQzB'

EQUITY_INFILE = 'wiki_stock_codes.json'
EQUITY_OUTFILE = 'wiki_stock_series.json'

class Fetcher(object):
	""" 
	A class to fetch data from the Quandl. 

	 * V1 fetches sample data
	 * V2 will interact with the DB to only fetch fresh data
	"""

	QUANDL_CLOSING_STOCK_PRICE_COLUMN_NUMBER = 10
	QUANDL_CLOSING_FX_COLUMN_NUMBER = 1

	def fetch_commodity_price_data(self):
		pass


	def _fetch_quandl_series(self, series, kind):
		if kind == 'fx': # make sure we're fetching the correct column
			column = self.QUANDL_CLOSING_STOCK_PRICE_COLUMN_NUMBER
		if kind == 'stock':
			column = self.QUANDL_CLOSING_FX_COLUMN_NUMBER
		data = q.get(series.upper(),  # fetch ze column!
					 trim_start='2010-03-05', 
					 column=column,
					 authtoken = QUANDL_API_KEY
					)
		name = series.split('/')[1]
		if column == 10:
			data.columns = pd.Index([name])
		if column == 1:
			data.columns = pd.Index([name])
		return data

	def fetch_stock_data(self):
		first = True
		codes = pd.read_json(EQUITY_INFILE)
		print codes.shape
		for i, code in enumerate(codes.iloc[:,0]):
			print "Fetching series %d of %d, %s" % (i, len(codes.index), code)
			series = self._fetch_quandl_series(code, kind='stock')
			if first:
				stock_frame = series
				first = False
			else:
				stock_frame = stock_frame.join(series)
			if i == 100:
				break
		return stock_frame

	def fetch_bitcoin_series(self):
		''' Fetches historical BTC price data from Coinbase, returns a Data Frame '''
		response = requests.get(COINBASE_ENDPOINT)
		if response:
		    data = response.content
		    btc_history = pd.read_json(data)
		btc_history.date = pd.to_datetime(btc_history.date)
		btc_history.set_index('date', inplace=True)
		btc_history.columns = pd.Index(['btc'])
		return btc_history	

def add_new_time_series_to_data_df(base_series, df):
	#TODO: Should check how many columns are in the base series. If it's only one
	#      make sure to call time_series_to_index on it
	first_merge = True
	new_frame = base_series.join(df) # join two dfs together
	series_name = df.dtypes.index[0] # get field name of non bitcoin series
	# fill missing non bitcoin data
	new_frame[series_name] = new_frame[series_name].fillna(method='pad')
	new_frame = new_frame.dropna() # drop remaining missing values
	# reindex the values in my data frame so they both begin at 100
	new_frame[new_frame.columns[0]] = new_frame
	df_name = df.columns[0] # get the name of the series we're joining to our base data frame
	new_frame[df_name] = time_series_to_index(new_frame, df_name)
	if first_merge:
		colname = base_series.columns[0]
		new_frame[colname] = time_series_to_index(new_frame, colname)
	return new_frame


def time_series_to_index(df, column_name):
	return  df[column_name] / df[column_name].iloc[0]

if __name__ == '__main__':
	f = Fetcher()

	# fetch and write stock data
	data = f.fetch_stock_data()
	data.to_json(EQUITY_OUTFILE)
	