import json
import time
from sys import argv


import numpy as np
import pandas as pd
import requests

from models import EconomicMetadata
from config import QUANDL_API_KEY
from connection_manager import DBConnect

'''
	A one-off ETL for filling the dim_series table, which contains metadata
	about our economic series. It will fetch the docs from Quandl, process them
	so we only have the information we need, and load them into the database.
'''

def fetch_quandl_docs(source, pages=1):
	''' Returns a data frame with metadata on quandl series '''
	DOCUMENTATION_ENDPOINT =  'https://www.quandl.com/api/v2/datasets.json?query=*&source_code=%s&per_page=300&page=%s&auth_token=%s' 
	raw_json = []
	for i in range(1, pages + 1):
		url = DOCUMENTATION_ENDPOINT % (source, i, QUANDL_API_KEY)
		print "Fetching page %s" % i
		response = requests.get(url)
		content = response.content
		try:
			json_obj = json.loads(content)
		except ValueError:
			print 'Failure:/n/n%s' % content
		raw_json.append(json_obj)
	return raw_json

def transform_google_docs(json_from_quandl):
	sources = [page['docs'] for page in json_from_quandl]
	list_of_sources = []
	for source in sources:
		list_of_sources.extend(source)
	df = pd.DataFrame(list_of_sources)
	df.set_index('code', inplace=True, drop=False)

	# focus on daily series from a limited set of sources
	google_sources = [code[0] for code in df.code.str.split('_').tolist()] # splits NASDAQ_FB into just NASDAQ
	df['google_source'] = google_sources
	df = df[(df.google_source=='NASDAQ')|(df.google_source=='AMEX')|(df.google_source=='NYSE')]
	df = df[df.frequency=='daily']

	#generate quandl codes
	quandl_codes = {}
	# instantiate a list to hold duplicated entries. Not exactly sure what's 
	# going on here, but I'm having trouble creating quandl_codes via a dict 
	# comprehension. Need to specify what to do in cases where the value is a 
	# string versus where the value is a Series
	duplicates = [] 
	for i in df.index:
		if len(df.ix[i]) > 2:
			new_code = {'{code}'.format(code=df.ix[i].code): '{source_code}/{code}'.format(source_code=df.ix[i].source_code, code=df.ix[i].code)}
			quandl_codes.update(new_code)
		else:
			duplicates.append(df.ix[i].code)
	duplicates = [value[0] for value in duplicates]
	new_codes = {'{code}'.format(code=val):'GOOG/{code}'.format(code=val) for val in duplicates}
	quandl_codes.update(new_codes)

	df['quandl_code'] = pd.Series(quandl_codes)
	df['series_name'] = df.name # rename name to series_name
	df.description = df.description.str.encode('utf-8') # encode description to unicode
	df = df[['quandl_code', 'description', 'series_name', 'source_code', 'to_date']]
	df.reset_index(inplace=True)
	return df

def load_docs_to_db(df):
	start = time.time()
	session = DBConnect().create_session()
	rows = [EconomicMetadata(code=df.ix[i].code, 
						 source_code=df.ix[i].source_code,
						 series_name=df.ix[i].series_name,
						 quandl_code=df.ix[i].quandl_code,
						 description=df.ix[i].description) for i in df.index]

	session.add_all(rows)
	session.commit()
	session.close()
	run_time = time.time() - start
	print "time to db: {num_seconds} seconds".format(num_seconds=run_time)
	return 0


if __name__ == '__main__':
	
	if len(argv) > 1:
		num_pages = int(argv[1])
		gdocs = fetch_quandl_docs('GOOG', pages=num_pages)
	else:
		gdocs = fetch_quandl_docs('GOOG', pages=247)
	docs = transform_google_docs(gdocs)
	load_docs_to_db(docs)
