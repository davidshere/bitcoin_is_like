from datetime import datetime, timedelta
from math import floor
import itertools
import random
import time

import numpy as np
import pandas as pd 
from sqlalchemy import func

import connection_manager as cm
from models import EconomicMetadata, EconomicSeries, Match

class MatchingAlgorithm(object):

    def __init__(self, start_date, end_date, matching_object, matching_series):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.matching_object = matching_object
        self.matching_series = matching_series

    def finding_series_with_too_many_nas(self, df, threshold=.9):
        ''' Returns a series ids with fewer nans then the threshold. So if 
            the threshold is .9, at least 90 percent of data in the series
            must be non null '''
        data_left = df.isnull().apply(pd.Series.value_counts).T
        return data_left[data_left[True] / data_left.apply(sum, axis=1) > threshold].index

    def reduce_raw_data_series(self):
        ''' Reduces the raw data set by clipping it by time 
            and removing any with too many nas in the time period
        '''
        padded_btc = self.matching_object.raw_btc.fillna(method='pad')
        padded_data = self.matching_object.raw_data[self.matching_series].fillna(method='pad')
        self.btc = padded_btc[(padded_btc.index >= self.start_date.date()) & (padded_btc.index <= self.end_date.date())]
        trimmed_data = padded_data[(padded_data.index >= self.start_date) & (padded_data.index <= self.end_date)]
        series_with_too_many_nas = self.finding_series_with_too_many_nas(padded_data)
        self.data = trimmed_data.drop(series_with_too_many_nas, axis=1)

    def prep_frame(self):
        ''' This method should be run each time you want to test a new start date '''
        # backfill missing data
        self.reduce_raw_data_series()
        self.length = (self.btc.index.max() - self.btc.index.min()).days
        self.btc = self.btc.values.reshape(-1, )

    def standardize(self, array):
        columnMeans = array.mean(axis=0)
        return array - columnMeans

    def algorithm(self):
        self.btc -= self.btc.mean()
        btc_std = self.btc.std()

        stock_prices_std = self.standardize(self.data)
        rolling_deviations_df = pd.rolling_std(self.data, self.length)
        print 'rolling deviations', self.start_date, self.end_date
        print rolling_deviations_df.shape
        print rolling_deviations_df.head()
        print rolling_deviations_df.apply(pd.Series.isnull).apply(pd.Series.value_counts)
        rolling_deviations_array = rolling_deviations_df.values[(self.length - 1):, :]

        correlationsList = []

        for colIdx in xrange(stock_prices_std.shape[1]):
            corr = np.correlate(stock_prices_std.iloc[:, colIdx].reshape((-1, )), self.btc)
            correlationsList.append(corr)

        corr_array = np.array(correlationsList).transpose()

        # for some reason, np.divide is haivng a weird broadcasting issue. Explore ASAP.
        print corr_array.shape[0], '\t', rolling_deviations_array.shape[0], '\t',
        try:
            corr_array = np.divide(corr_array, rolling_deviations_array * btc_std)
            print 'success'
        except:
            import pdb
            pdb.set_trace()
            print 'failure'


        # For reasons that evade me, your data have some series that don't vary for a 60-day period
        # This makes for a zero denominator in the correlation equation. No can haz.
        # Instead of intelligent exception handling, I'm just removing them.
        # Really, this should probably happen BEFORE we divide by zero, but it's late and I workout early, so we're getting pretty hacky here.
        #corr_array = np.where(corr_array != float('inf'), corr_array, 0)

        # max across correlations array
        #raw_idx = corr_array.argmax()
        #cols = corr_array.shape[1]
        #row_id = int(floor(raw_idx / cols))
        #col_id = raw_idx % cols

        # get matching series id
        #matching_series = stock_prices_std.iloc[row_id:(row_id + self.length),col_id].name

        #return matching_series


    def match(self):
        self.prep_frame()
        match = self.algorithm()
        return {'start_date': self.start_date, 'end_date': self.end_date, 'series_id':match}


class Matcher(object):
    ''' Should mirror Fetcher in that it reads from a table, determines 
        which dates are missing, and fills in the matches for the missing
        dates. Uses a MatchingAlgorithm object to identify matches. '''

    def __init__(self):
        self.session = cm.DBConnect().create_session()
        self.engine = self.session.connection().engine
        self.matches = []

    def load_data(self):
        query = 'select * from cust_series;'
        # find bitcoin series id
        btc_id = self.session.query(EconomicMetadata.id).filter(EconomicMetadata.quandl_code==None).one()[0]
        price_series =  pd.read_sql_query(query, self.engine)
        btc = price_series[price_series.series_id==btc_id] # pull bitcoin data out of data frame into its own
        self.raw_btc = btc[['date', 'value']].set_index('date')
        self.raw_data = price_series.pivot(index='date', columns='series_id', values='value')

    def get_series_date_ranges(self):
        date_ranges = {series_id: {
                            'max': self.raw_data[series_id].dropna().index.max(),
                            'min': self.raw_data[series_id].dropna().index.min() 
                        } for series_id in self.raw_data}
        self.date_ranges = pd.DataFrame(date_ranges)

    def get_series_to_match(self, start, end):
        ''' gets the last day of each series, so we're only passing series
            with enough data '''
        to_send = ((self.date_ranges.ix['max'] >= end) & (self.date_ranges.ix['min'] <= start))
        return self.date_ranges.T.ix[to_send].index

    def get_dates_from_data(self):
        all_dates = self.session.query(func.distinct(EconomicSeries.date)).all()
        return map(lambda x: x[0], all_dates)

    def get_existing_pairs(self):
        existing_pairs = self.session.query(Match.start_date, Match.end_date).all()
        return set(existing_pairs)

    def get_date_pairs(self):
        all_dates = self.get_dates_from_data()
        old_pairs = self.get_existing_pairs()
        for start, end in itertools.product(all_dates, all_dates):
            if end - start > timedelta(30) and (start, end) not in old_pairs:
                yield (start, end)

    def match_days(self, batch=False):
        ''' Iterates through pairs of days, and matches them if they're unmatched.
            
            If batch=False it will dump the matches one at a time to the db, otherwise
            it'll do them all at once '''
        # TODO: Remove this
        self.batch = batch
        pairs = [pair for pair in self.get_date_pairs()]
        random_pairs = random.sample(pairs, 50)
        for start, end in random_pairs:

        #for start, end in self.get_date_pairs():
            matching_series = self.get_series_to_match(start, end)
            algo = MatchingAlgorithm(start, end, self, matching_series)
            match = algo.match()
            # if batch:
            #      self.matches.append(match)
            #  else:
            #      self.write_individual_match_to_db(match)

    def write_matches_to_db(self):
        ''' Should write the new matches from match_days to fact_match '''
        self.engine.execute(Match.__table__.insert(), self.matches)

    def write_individual_match_to_db(self, match):
        self.engine.execute(Match.__table__.insert(), match)


    def run_matcher(self):
        ''' 1) Loads raw data from the DB, 
            2) Puts together a set of dates, 
            3) Runs the matching algorithm
            4) Writes matches to DB '''
        self.load_data()
        self.get_series_date_ranges()
        return self.match_days()
        return self.matches
        if self.batch:
            self.write_matches_to_db()


        


if __name__ == '__main__':

    start_time = time.time()
    m = Matcher()
    r = m.run_matcher()

    '''
    m.run_matcher()
    end_time = time.time()
    minutes = (end_time - start_time) / 60
    print "\nMatcher Duration: {mins} minutes".format(mins=minutes)
    '''