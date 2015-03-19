from datetime import datetime
import json	
import sys
		
import requests
import pandas as pd	
import numpy as np

import fred
import Quandl as q
from config import FRED_API_KEY


COINBASE_ENDPOINT = 'https://www.coinbase.com/charts/price_history?days=365'
COINBASE_ENDPOINT_VARIABLE_DAYS = 'https://www.coinbase.com/charts/price_history?days=%s'

QUANDL_ENDPOINT = 'https://www.quandl.com/api/v1/datasets/%s/%s?sort_order=json.asc'
QUANDL_CLOSING_PRICE_COLUMN_NUMBER = 10
QUANDL_API_KEY = 'XL6P_9uvtZeqd6fFBQzB'


#initializing fred with my api key
fred.key(FRED_API_KEY)

# FX values should be in the number of dollars per unit of foreign currency
FRED_FX_NAMES = {
	'fx_us_euro': 'dexuseu',
	'fx_us_uk': 'dexusuk',
}


QUANDL_SERIES = {
	'ECB': {
		'Australian Dollar (AUD)':'EURAUD',
		'Bulgarian Lev (BGN)':'EURBGN',
		'Brazilian Real (BRL)':'EURBRL',
		'Canadian Dollar (CAD)':'EURCAD',
		'Swiss Franc (CHF)':'EURCHF',
		'Chinese Yuan (CNY)':'EURCNY',
		'Czech Koruna (CZK)':'EURCZK',
		'Danish Krone (DKK)':'EURDKK',
		'British Pound (GBP)':'EURGBP',
		'Hong Kong Dollar (HKD)':'EURHKD',
		'Croatian Kuna (HRK)':'EURHRK',
		'Hungarian Forint (HUF)':'EURHUF',
		'Indonesian Rupiah (IDR)':'EURIDR',
		'New Israeli Shekel (ILS)':'EURILS',
		'Indian Rupee (INR)':'EURINR',
		'Icelandic Krona (ISK)':'EURISK',
		'Japanese Yen (JPY)':'EURJPY',
		'Korean Won (KRW)':'EURKRW',
		'Lithuanian Lita (LTL)':'EURLTL',
		'Mexican Peso (MXN)':'EURMXN',
		'Malaysian Ringgit (MYR)':'EURMYR',
		'Norwegian Krone (NOK)':'EURNOK',
		'New Zealand Dollar (NZD)':'EURNZD',
		'Philippine Peso (PHP)':'EURPHP',
		'Polish Zloty (PLN)':'EURPLN',
		'Romanian Leu (RON)':'EURRON',
		'Russian Rubble (RUB)':'EURRUB',
		'Swedish Krona (SEK)':'EURSEK',
		'Singapore Dollar (SGD)':'EURSGD',
		'Thai Baht (THB)':'EURTHB',
		'Turkish Lira (TRY)':'EURTRY',
		'US Dollar (USD)':'EURUSD',
		'South African Rand (ZAR)':'EURZAR'
	}
}


def fetch_quandl_series(series, kind):
	if kind == 'fx': # make sure we're fetching the correct column
		column = 1
	if kind == 'stock':
		column = 10

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


STOCK_CODE_FILENAME = 'wiki_stock_codes.json'

class Fetcher(object):

	def fetch_commodity_price_data(self):
		pass

	def fetch_stock_data(self):
		first = True
		codes = pd.read_json('wiki_stock_codes.json')
		print codes.shape
		for i, code in enumerate(codes.iloc[:,0]):
			print "Fetching series %d of %d, %s" % (i, len(codes.index), code)
			series = fetch_quandl_series(code, kind='stock')
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
	first_merge = False
	if len(base_series.columns) == 1: # if we haven't joined any series together yet, we need to make sure to index the base_series
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

def fetch_all_ecb_quandl_data():
		df = pd.DataFrame()
		for series in QUANDL_SERIES['ECB']:
			print series	
			s = QUANDL_SERIES['ECB'][series]
			param = '%s/%s' % ('ECB', s)
			data = fetch_quandl_series(param, 'fx')
			if df.size == 0:
				df = data
			else:
				try:
					df = add_new_time_series_to_data_df(df, data)
				except:
					print '%s failed join' % series
		return df

if __name__ == '__main__':
	f = Fetcher()
	data = f.fetch_stock_data()
	print data.head()
	