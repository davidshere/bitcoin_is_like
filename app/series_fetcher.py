import pandas as pd
from sqlalchemy import func

from connection_manager import DBConnect
from models import EconomicSeries, EconomicMetadata, Match

class SeriesFetcher(object):

	def __init__(self):
		self.session = DBConnect().create_session()
		self.match_dict = {}

	def fetch_bitcoin_data(self):
		btc_id = self.session.query(EconomicMetadata.id).filter(EconomicMetadata.quandl_code==None).one()[0]
		query = self.session.query(EconomicSeries.date, EconomicSeries.value)
		filtered_query = query.filter(EconomicSeries.series_id==btc_id)
		result = filtered_query.all()
		self.result = result
		return result

	def fetch_bitcoin_series(self):
		result = self.fetch_bitcoin_data()
		self.processed_btc = {row[0].isoformat(): float(row[1]) for row in result}
		return self.processed_btc

	def fetch_match_id(self, start_date, end_date):
		self.start_date = start_date
		self.end_date = end_date
		query = self.session.query(Match.series_id)
		filtered = query.filter((Match.start_date==self.start_date) & (Match.end_date==self.end_date))
		self.match_id = filtered.one()[0]

	def fetch_match_series(self):
		series_query = self.session.query(EconomicSeries.date, EconomicSeries.value)
		query_with_filter = series_query.filter((EconomicSeries.series_id==self.match_id) & (EconomicSeries.date > self.start_date))
		result = query_with_filter.all()
		match_df = pd.DataFrame(result)
		match_df.columns = pd.Index(['date', 'series'])
		match_df.set_index('date', drop=False, inplace=True)
		match_df.index = pd.to_datetime(match_df.index)
		self.match_df = match_df
	
	def fetch_match_metadata(self):
		query = self.session.query(EconomicMetadata.series_name)
		query_with_filter = query.filter(EconomicMetadata.id == self.match_id)
		result = query_with_filter.one()
		self.match_dict['company_name'] = result

	def align_match(self):
		self.btc = self.fetch_bitcoin_series() # bring in the btc data
		self.btc = pd.Series(self.btc)
		self.btc.index = pd.to_datetime(self.btc.index) # change the index's data type
		match = self.match_df
		match['btc'] = self.btc
		match = match[match.index >= match.index.min()] # cut off the date to the starting date
		match = match.fillna(method='ffill') # fill in missing data
		self.match_data = match.dropna() # drop remaining missing data
		# reindex data

	def transform_aligned_match_df(self):
		match_series_dict = self.match_data.to_dict(orient='list')
		match_series_dict['date'] = [date.isoformat() for date in match_series_dict['date']]
		match_series_dict['series'] = [float(value) for value in match_series_dict['series']]
		self.match_dict['series'] = match_series_dict

	def fetch_match(self, start_date, end_date):
		self.fetch_match_id(start_date, end_date)
		self.fetch_match_series()
		self.fetch_match_metadata()
		self.align_match()
		self.transform_aligned_match_df()
		return self.match_dict

	def fetch_last_match(self):
		match_date_range = self.session.query(func.max(Match.start_date), func.min(Match.start_date)).one()
		earliest = match_date_range[0].isoformat()
		latest = match_date_range[1].isoformat()
		return {'earliest': earliest, 'latest': latest}

	def _process_match_series_dict(self, result):
		pass


if __name__ == '__main__':
	f = SeriesFetcher()
	btc = f.fetch_bitcoin_series()
	return_series = f.fetch_match('2014-01-01')