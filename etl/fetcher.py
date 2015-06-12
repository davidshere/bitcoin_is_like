from datetime import datetime, timedelta
import json	
import sys
import time
		
import numpy as np
import pandas as pd
import Quandl as q
import requests
from sqlalchemy import update, func

from connection_manager import DBConnect
from etl.config import QUANDL_API_KEY
from etl.models import EconomicMetadata, EconomicSeries

class Fetcher(object):
	'''
		Class to update the cust_series table.

		1. Fetches, for each series in dim_series, the series code and the last_updated date
		2. For each code, fetches the data from the last_updated date
		3. Writes new and updated data to the data base
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

	def _parse_bitcoinaverage_datetime(self, timestamp):
		''' Turn Fri, 29 May 2015 05:51:06 -0000 into 2015-05-29 '''
		dt = timestamp.split()
		dtlist = [dt[3], dt[2], dt[1]]
		datestr = ' '.join(dtlist)
		datestruct = time.strptime(datestr, '%Y %b %d')
		date_time = time.mktime(datestruct)
		dt = datetime.fromtimestamp(date_time)
		return dt

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

	def fetch_single_latest_quandl(self, series_metadata):
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

		# check if there's a last updated date, then fetch the data
		if series_metadata['last_updated']:
			next = series_metadata['last_updated'] + timedelta(days=1)
			# THIS IS CRAPPY, YOU SHOULD FIX IT AT SOME POINT
			try: # protect against exceptions when hitting the Quandl API
				data = self._fetch_quandl_series(qcode, source, start=next)
			except:
				return None
		else:
			data = self._fetch_quandl_series(qcode, source)

		# transform the data
		data.columns = pd.Index(['value'])
		data['series_id'] = id
		data['date'] = data.index

		path = '{folder}/{qcode}.json'.format(folder='backfilled_data', qcode=id)
		data.to_json(path)
		return data.to_dict(orient='records')

	def fetch_bitcoin_average(self, series_id):
		''' Bitcoin price data comes from api.bitcoinaverage.com '''
		url = 'https://api.bitcoinaverage.com/ticker/global/USD/'
		try:
			response = requests.get(url, timeout=1)
		except ConnectTimeout:
			return None

		response = response.json()
		price = response['24h_avg']
		dt = response['timestamp']
		dt = self._parse_bitcoinaverage_datetime(dt)
		return_dict = {
			'value': price,
			'series_id': series_id,
			'date': dt
		}
		return return_dict 

	def fetch_all_fresh_series(self, economic_metadata):
		fresh_data = list()
		for i, series in enumerate(economic_metadata):
			print i, series
			if series['quandl_code']: # check if there's a quandl code (btc won't have one)
				fresh = self.fetch_single_latest_quandl(series)
				#if fresh: # in case no data came back
				#	fresh_data.extend(fresh)
			else:
				series_id = series['id']
				fresh = self.fetch_bitcoin_average(series_id)
				#fresh_data.append(fresh)
		return fresh_data

	def write_economic_data_to_db(self, updated_data):
		''' Should write the new values from fetch_latest to cust_series '''
		economic_series = [EconomicSeries(series_id=datapoint['series_id'],
									  date=datapoint['date'],
									  value=datapoint['value']) 
									  for datapoint in updated_data]
		self.session.add_all(economic_series)
		self.session.commit()
		self.session.close()
		return 0

	def run_stored_procedures(self):
		sp_list = ['sp_updated_freshest_date']
		# sp_list.append('sp_delete_duplicates')
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
		print len(last_updated)
		updated_data = self.fetch_all_fresh_series(last_updated)
		self.write_economic_data_to_db(updated_data)
		self.run_stored_procedures()
		return 0


def write_to_db_from_json_filenames(foldername):
	''' Function to write data to DB from json files in a folder in the working directory'''
	path_to_names = '{basedir}/{folder}/'.format(basedir=os.getcwd(), folder=foldername)
	filenames = os.listdir(path_to_names)
	full_paths =  ['backfilled_data/{file}'.format(path=path_to_names, file=filename) for filename in filenames]
	for filename in full_paths:
		print filename
		try:
			updated_data = pd.read_json(filename).to_dict(orient='records')
		except ValueError:
			print filename
			continue
		f.write_economic_data_to_db(updated_data)



if __name__ == '__main__':

	f =	Fetcher()
	f.update()


