from sqlalchemy import func

from connection_manager import DBConnect
from models import EconomicSeries, EconomicMetadata, Match

class SeriesFetcher(object):

	def __init__(self):
		self.session = DBConnect().create_session()
		self.match_dict = {}

	def fetch_bitcoin_series(self):
		btc_id = self.session.query(EconomicMetadata.id).filter(EconomicMetadata.quandl_code==None).one()[0]
		query = self.session.query(EconomicSeries.date, EconomicSeries.value)
		filtered_query = query.filter(EconomicSeries.series_id==btc_id)
		result = filtered_query.all()
		processed_result = self._process_series_query_results(result) # datetime into string, Decimal into float
		return processed_result	

	def fetch_match_id(self, start_date):
		self.start_date = start_date[0]
		self.match_id = self.session.query(Match.series_id).filter(Match.start_date==self.start_date).one()[0]

	def fetch_match_series(self):
		series_query = self.session.query(EconomicSeries.date, EconomicSeries.value)
		query_with_filter = series_query.filter((EconomicSeries.series_id==self.match_id) & (EconomicSeries.date > self.start_date))
		result = query_with_filter.all()
		processed_result = self._process_series_query_results(result) 
		self.match_dict['series'] = processed_result
	
	def fetch_match_metadata(self):
		query = self.session.query(EconomicMetadata.series_name)
		query_with_filter = query.filter(EconomicMetadata.id == self.match_id)
		result = query_with_filter.one()
		self.match_dict['company_name'] = result

	def fetch_match(self, start_date):
		self.fetch_match_id(start_date)
		self.fetch_match_series()
		self.fetch_match_metadata()
		return self.match_dict

	def fetch_last_match(self):
		last_match = DBConnect().create_session().query(func.max(Match.start_date)).one()[0]
		return {'date': last_match.isoformat()}


	def _process_series_query_results(self, result):
		return {row[0].isoformat(): float(row[1]) for row in result}


if __name__ == '__main__':
	f = SeriesFetcher()
	date = f.fetch_last_match()
	print date
	