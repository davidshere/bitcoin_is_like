from datetime import datetime, timedelta
import json	
import sys
import time
		
import numpy as np
import pandas as pd
import Quandl as q
import requests
from sqlalchemy import update, func

from config import QUANDL_API_KEY
from connection_manager import DBConnect
from models import EconomicMetadata, EconomicSeries


COINBASE_ENDPOINT = 'https://www.coinbase.com/charts/price_history?days=%s'

class Fetcher(object):
	'''
		Class to update the cust_series table.

		1. Fetches, for each series in dim_series, the series code and the last_updated date
		2. For each code, fetches the data from the last_updated date
		3. Appends updates to cust_series table

		Also contains helper methods for fetching other things, including documentation.
	'''

	SOURCE_RELELVANT_COLUMN_MAP = {'WIKI': 10,
								   'WSJ': 1,
								   'OFDP': 4,
								   'ODA': 1,
								   'WORLDBANK': 1,
								   'GOOG': 1,
								   'BRP': 1,
								   'BNP': 1,
								   'BUNDESBANK': 1,
								   'DOE': 1,
								   'FRED': 1 }

	def __init__(self):
		self.session = DBConnect().create_session()

	def _fetch_quandl_series(self, series, source, start='2010-07-17'):
		column = self.SOURCE_RELELVANT_COLUMN_MAP[source]
		response = q.get(series.upper(), trim_start=start, column=column, authtoken=QUANDL_API_KEY)
		name = series.split('/')[1]
		response.columns = pd.Index([name])
		return response

	def fetch_last_updated_dates(self):
		''' A method to query dim_series for the last updated dates for each series '''
		last_updated = self.session.query(EconomicMetadata.id,
										  EconomicMetadata.quandl_code,
										  EconomicMetadata.source_code,
										  EconomicMetadata.last_updated).all()
		keys = ['id', 'quandl_code', 'source_code', 'last_updated']
		list_of_dicts = [dict(zip(keys, row)) for row in last_updated]
		return list_of_dicts

	def fetch_single_latest(self, series_metadata):
		''' 
			Fetches updated data from Quandl based on the metadata.

			Returns a Data Frame in the shape it's supposed to be in for the
			db.

			1. Fetch Quandl codes and last updated dates
			2. For each Quandl Code:
				2a. If there is a last updated date, fetch the fresh data
				2b. If there is no last updated date, fetch all data past a given start date

		'''
		source = series_metadata['source_code']
		qcode = series_metadata['quandl_code']
		id = series_metadata['id']

		# fetch the data
		if series_metadata['last_updated']:
			next = series_metadata['last_updated'] + timedelta(days=1)
			data = self._fetch_quandl_series(qcode, source, start=next)
		else:
			data = self._fetch_quandl_series(qcode, source)

		# transform the data
		data.columns = pd.Index(['value'])
		data['series_id'] = id
		data['date'] = data.index
		return data.to_dict(orient='records')

	def fetch_all_fresh_series(self, economic_metadata):
		fresh_data = list()
		for i, series in enumerate(economic_metadata):
			print i
			fresh = f.fetch_single_latest(series)
			fresh_data.extend(fresh)
		return fresh_data

	def write_economic_data_to_db(self, updated_data):
		''' Should write the new values from fetch_latest to cust_series,
			and update the last_updated field in dim_series
		'''
		emd_objects = [EconomicSeries(series_id=datapoint['series_id'],
									  date=datapoint['date'],
									  value=datapoint['value']) 
									  for datapoint in updated_data]
		self.session.add_all(emd_objects)
		self.session.commit()
		return 0

	def update_last_updated_fields(self):
		conn = self.session.connection()
		most_recent = self.session.query(EconomicSeries.series_id, 
						func.max(EconomicSeries.date))\
			.group_by(EconomicSeries.series_id).all()

		
		update_statements = []
		for series_id, date in most_recent:
			statement = update(EconomicMetadata).where(EconomicMetadata.id==series_id).values(last_updated=date)
			update_statements.append(statement)
		return update_statements
		self.session.commit()
		conn.close()
		return 0

	def run_stored_procedures(self):
		sp_list = ['sp_updated_freshest_date']
		conn = self.session.bind.raw_connection()
		try:
			cursor = conn.cursor()
			for sp in sp_list:
				cursor.callproc(sp)
			cursor.close()
			conn.commit()
		finally:	
			conn.close()


	def update(self):
		''' This should be run from run_etl.py. '''
		last_updated = self.fetch_last_updated_dates()
		updated_data = f.fetch_all_fresh_series(last_updated)
		self.write_economic_data_to_db(updated_data)
		self.run_stored_procedures() # runs a procedure to update our last updated fields
		return 0

	def fetch_bitcoin_series(self, days=999):
		''' Fetches historical BTC price data from Coinbase, returns a Series '''
		print COINBASE_ENDPOINT % days
		response = requests.get(COINBASE_ENDPOINT % days)
		print response
		if response:
		    data = response.content
		    btc_history = pd.read_json(data)
		else:
			return None
		btc_history.date = pd.to_datetime(btc_history.date)
		btc_history.set_index('date', inplace=True)
		btc_history.columns = pd.Index(['btc'])
		return btc_history.btc

if __name__ == '__main__':

	f =	Fetcher()

	#last_updated = f.fetch_last_updated_dates()
	#updated_data = f.fetch_all_fresh_series(last_updated)
	#f.write_economic_data_to_db(updated_data)
	f.update()



	'''
	a = last_updated[0]
	print a
	id = a[0]
	qcode = a[1]
	source = a[2]
	updated = a[3]
	x = f._fetch_quandl_series(qcode, source)



	'''
	