from datetime import datetime
import json	
import sys
		
import requests
import pandas as pd	
import numpy as np

import Quandl as q
from config import QUANDL_API_KEY


COINBASE_ENDPOINT = 'https://www.coinbase.com/charts/price_history?days=365'
COINBASE_ENDPOINT_VARIABLE_DAYS = 'https://www.coinbase.com/charts/price_history?days=%s'

QUANDL_ENDPOINT = 'https://www.quandl.com/api/v1/datasets/%s/%s?sort_order=json.asc'

# global variables that don't represent core functionality
EQUITY_INFILE = 'wiki_stock_codes.json'
EQUITY_OUTFILE = 'wiki_stock_series.csv'

COMMODITY_INFILE = 'commodities.csv'
COMMODITY_OUTFILE = 'wsj_commodity_series.csv'

FX_INFILE = 'bnpparibas_docs.csv'
FX_OUTFILE = 'bnp_fx_series.csv'

class Fetcher(object):
	""" 
	A class to fetch data from the Quandl. 

	 * V1 fetches sample data
	 * V2 will interact with the DB to only fetch fresh data
	   V2 will also pull the metadata on the series we track (i.e. its Source) from a db table
	"""

	#QUANDL_CLOSING_STOCK_PRICE_COLUMN_NUMBER = 10
	#QUANDL_CLOSING_FX_COLUMN_NUMBER = 1

	SOURCE_RELELVANT_COLUMN_MAP = {'WIKI': 10,
								   'WSJ': 1,
								   'OFDP': 4,
								   'ODA': 1,
								   'WORLDBANK': 1,
								   'BRP': 1,
								   'BNP': 1,
								   'BUNDESBANK': 1,
								   'DOE': 1,
								   'FRED': 1 }

	def _fetch_quandl_series(self, series, source):
		column = self.SOURCE_RELELVANT_COLUMN_MAP[source]
		response = q.get(series.upper(), trim_start='2010-07-17', column=column, authtoken = QUANDL_API_KEY)
		
		name = series.split('/')[1]
		response.columns = pd.Index([name])
		return response

	def fetch_equity_series(self):
		first = True
		codes = pd.read_json(EQUITY_INFILE)
		print codes.shape
		for i, code in enumerate(codes.iloc[:,0]):
			print "Fetching series %d of %d, %s" % (i, len(codes.index), code)
			series = self._fetch_quandl_series(code, source='WIKI')
			if first:
				stock_frame = series
				first = False
			else:
				stock_frame = stock_frame.join(series)
			if i == 100: # we don't need the entire series
				break
		return stock_frame

	def fetch_commodity_series(self):
		''' For this test fetch, we're just going to fetch WSJ data, for simplicity '''
		first = True
		codes = pd.read_csv(COMMODITY_INFILE)
		wsj_codes = codes[codes.Source=='WSJ'].Code
		for i, code in enumerate(wsj_codes):
			print "Fetching commodity series %d of %d, %s" % (i, len(codes.index), code)
			series = self._fetch_quandl_series(code, source='WSJ')
			if first:
				commodity_frame = series
				first = False
			else:
			    try:
			    	commodity_frame = commodity_frame.join(series)
			    except ValueError:
			    	pass

			if i == 100: # we don't need the entire series
				break
		return commodity_frame

	def fetch_fx_series(self):
		first = True
		codes = pd.read_csv(FX_INFILE).code
		dollar_price_codes = []
		for code in codes:
			if code[3:]=='USD':
				dollar_price_codes.append(code)
		for i, code in enumerate(dollar_price_codes):
			print "Fetching fx series %d of %d, %s" % (i, len(dollar_price_codes), code)
			full_code = 'BNP/%s' % code
			series = self._fetch_quandl_series(full_code, source='BNP')
			if first:
				fx_frame = series
				first = False
			else:
			    try:
			    	fx_frame = fx_frame.join(series)
			    except ValueError:
			    	pass

			if i == 100: # we don't need the entire series
				break
		return fx_frame

	def fetch_bitcoin_series(self, days=1800):
		''' Fetches historical BTC price data from Coinbase, returns a Data Frame '''
		response = requests.get(COINBASE_ENDPOINT_VARIABLE_DAYS % days)
		if response:
		    data = response.content
		    btc_history = pd.read_json(data)
		btc_history.date = pd.to_datetime(btc_history.date)
		btc_history.set_index('date', inplace=True)
		btc_history.columns = pd.Index(['btc'])
		return btc_history	

def fetch_quandl_docs(format_string, pages=1):
	''' Returns a data frame with metadata on quandl series '''
	docs = []
	for i in range(1, pages + 1):
		response = requests.get(format_string % i)
		content = response.content
		json_obj = json.loads(content)
		docs.append(json_obj)
	sources = [page['docs'] for page in docs]
	list_of_sources = []
	for source in sources:
		list_of_sources.extend(source)
	df = pd.DataFrame(list_of_sources)
	df = df[['code', 'description', 'frequency', 'from_date', 'name', 'source_code', 'to_date', 'type']]	
	return df

if __name__ == '__main__':

	f = Fetcher()

	equity = f.fetch_equity_series()
	equity.to_csv(EQUITY_OUTFILE)

	commodity = f.fetch_commodity_series()
	commodity.to_csv(COMMODITY_OUTFILE)

	fx = f.fetch_fx_series()
	fx.to_csv(FX_OUTFILE)
	