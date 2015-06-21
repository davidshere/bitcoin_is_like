from datetime import datetime, timedelta
import json	
import os
import sys
import time
		
import numpy as np
import pandas as pd
import Quandl as q
import requests
from sqlalchemy import update, func

from connection_manager import DBConnect
from etl.config import QUANDL_API_KEY
from models import EconomicMetadata, EconomicSeries

FETCHED_DATA_FOLDER = 'fetched_data'

class FetcherBase(object):
	def __init__(self):
		self.session = DBConnect().create_session()
		self.check_or_create_destination_folder()

	def check_or_create_destination_folder(self):
		if not os.path.exists(FETCHED_DATA_FOLDER):
			os.makedirs(FETCHED_DATA_FOLDER)


class FetchQuandl(FetcherBase):
	''' Class to fetch a series from the Quandl API, transform it, and write
		the results to a .json file '''

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

	def _fetch_quandl_series(self, series, source, start='2010-07-17'):
		column = self.SOURCE_RELELVANT_COLUMN_MAP[source]
		response = q.get(series.upper(), trim_start=start, column=column, authtoken=QUANDL_API_KEY)
		name = series.split('/')[1]
		response.columns = pd.Index([name])
		return response

	def add_metadata(self, metadata):
		self.metadata = metadata

	def fetch_single_latest_quandl(self):
		''' 
			Fetches updated data from Quandl based on the metadata.

			1. Fetch Quandl codes and last updated dates
			2. For each Quandl Code:
				2a. If there is a last updated date, fetch the fresh data
				2b. If there is no last updated date, fetch all data past a given start date

		'''
		source = self.metadata['source_code']
		qcode = self.metadata['quandl_code']
		# check if there's a last updated date, then fetch the data
		if self.metadata['last_updated']:
			next = self.metadata['last_updated'] + timedelta(days=1)
			# THIS IS CRAPPY, YOU SHOULD FIX IT AT SOME POINT
			try: # protect against exceptions when hitting the Quandl API
				self.data = self._fetch_quandl_series(qcode, source, start=next)
			except:
				return None
		else:
			self.data = self._fetch_quandl_series(qcode, source)

	def transform(self):
		self.data.columns = pd.Index(['value'])
		self.data['series_id'] = self.metadata['id']
		self.data['date'] = self.data.index

	def write_to_json(self):
		path = '{folder}/{qcode}.json'.format(folder=FETCHED_DATA_FOLDER, qcode=self.metadata['id'])
		self.data.to_json(path)

	def fetch(self):
		self.fetch_single_latest_quandl()
		self.transform()
		self.write_to_json()


class FetchBTC(FetcherBase):
	''' Class to fetch bitcoin price data from bitcoinaverage.com, transform
		it, and write the results to .json file '''

	def _parse_bitcoinaverage_datetime(self, timestamp):
		''' Turn 2010-07-17 00:00:00 into a python date object '''
		dt = timestamp.split()[0].split('-')
		dtlist = [dt[0], dt[1], dt[2]]
		datestr = ' '.join(dtlist)
		datestruct = time.strptime(datestr, '%Y %m %d')
		date_time = time.mktime(datestruct)
		dt = datetime.fromtimestamp(date_time)
		dt = dt.date()
		return dt

	def fetch_bitcoin_file(self):
		# fetch json object with different choices for historical data
		url = 'https://api.bitcoinaverage.com/history/USD/'
		response = requests.get(url)
		options = response.json()
		csv_url = options['all_time']
		# fetch csv file
		response = requests.get(csv_url)
		content = response.content
		rows = content.splitlines()
		rows = map(lambda x: x.split(','), rows)[1:] # first row is a header
		return rows

	def fetch_bitcoin_metadata(self):
		btc_query = self.session.query(EconomicMetadata.id, EconomicMetadata.last_updated)
		filtered_query = btc_query.filter(EconomicMetadata.quandl_code == None)
		result = filtered_query.one()
		series_id = result[0]
		last_updated = result[1]
		self.metadata = {'series_id': series_id, 'last_updated': last_updated}
		return self.metadata

	def transform_bitcoin_data(self, metadata, rows):
		''' Processes date strings into date objects and truncates data to only the most recent values '''
		transformed_rows = [{'date': self._parse_bitcoinaverage_datetime(row[0]), 'value':row[-2]} for row in rows]
		if metadata['last_updated']:
			value_dicts = [{'date': row['date'],'series_id': metadata['series_id'],'value': row['value']} for row in transformed_rows if row['date'] > metadata['last_updated']]
		else:
			value_dicts = [{'date': row['date'],'series_id': metadata['series_id'],'value': row['value']} for row in transformed_rows]
		return value_dicts

	def write_bitcoin_data_to_json(self, data):
		if data: # check if anything was returned - if BTC series is totally fresh, nothing will be
			df = pd.DataFrame(data).set_index('date', drop=False)
			series_id = self.metadata['series_id']
			path = '{folder}/{series_id}.json'.format(folder=FETCHED_DATA_FOLDER, series_id=series_id)
			df.to_json(path)

	def fetch_bitcoin_data(self):
		rows = self.fetch_bitcoin_file()
		metadata = self.fetch_bitcoin_metadata()
		dicts = self.transform_bitcoin_data(metadata=metadata, rows=rows)
		if dicts:
			self.write_bitcoin_data_to_json(dicts)

class Fetcher(FetcherBase):
	'''
		Class to update the cust_series table.

		1. Fetches, for each series in dim_series, the series code and the last_updated date
		2. For each code, fetches the data from the last_updated date
		3. Writes new and updated data to the data base
	'''

	def fetch_last_updated_dates(self, backfill=False):
		''' A method to query dim_series for the last updated dates for each series 

			If backfill=True, we're only pulling metadata on series which have no 
			presence in cust_series
		'''
		if backfill:
			last_updated = self.session.query(EconomicMetadata.id,
											  EconomicMetadata.quandl_code,
											  EconomicMetadata.source_code,
											  EconomicMetadata.last_updated).filter(
											  EconomicMetadata.last_updated==None).all()
		else:
			last_updated = self.session.query(EconomicMetadata.id,
											  EconomicMetadata.quandl_code,
											  EconomicMetadata.source_code,
											  EconomicMetadata.last_updated).all()

		keys = ['id', 'quandl_code', 'source_code', 'last_updated']
		list_of_dicts = [dict(zip(keys, row)) for row in last_updated]
		return list_of_dicts

	def fetch_all_fresh_series(self, economic_metadata, recently_updated=True):
		for i, series in enumerate(economic_metadata):
			print i, series
			if series['quandl_code']: # check if there's a quandl code (btc won't have one)
				quandl_fetcher = FetchQuandl()
				quandl_fetcher.add_metadata(series)
				quandl_fetcher.fetch()
			else:
				btc_fetcher = FetchBTC()
				btc_fetcher.fetch_bitcoin_data()
		return 0

	def _write_economic_dicts_to_db(self, dicts_to_write):
		''' Should write the new values from fetch_latest to cust_series '''
		economic_series = [EconomicSeries(series_id=datapoint['series_id'],
									  date=datapoint['date'],
									  value=datapoint['value']) 
									  for datapoint in dicts_to_write]
		self.session.add_all(economic_series)
		self.session.commit()
		self.session.close()
		return 0

	def write_to_db_from_json_filenames(self):
		''' Function to write data to DB from json files in a folder in the working directory'''
		path_to_names = '{basedir}/{folder}/'.format(basedir=os.getcwd(), folder=FETCHED_DATA_FOLDER)
		filenames = os.listdir(path_to_names)
		full_paths =  ['{path}/{file}'.format(path=path_to_names, file=filename) for filename in filenames]
		for filename in full_paths:
			try:
				updated_data = pd.read_json(filename).to_dict(orient='records')
			except ValueError:
				continue
			self._write_economic_dicts_to_db(updated_data)

	def run_stored_procedures(self):
		sp_list = ['sp_updated_freshest_date',
  				   'sp_delete_duplicates_from_cust_series'
				]
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
		self.fetch_all_fresh_series(last_updated)
		self.write_to_db_from_json_filenames()
		self.run_stored_procedures()
		return 0



if __name__ == '__main__':

	f =	Fetcher()
	f.update()
	#historical_btc = f.fetch_historical_bitcoin_data()
	#f.write_economic_data_to_db(historical_btc)
	#d = f.fetch_bitcoin_average(10522)
	#print d
	#f.update()


