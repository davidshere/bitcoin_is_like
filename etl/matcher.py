from datetime import datetime, timedelta
import itertools
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
        max_btc = self.matching_object.raw_btc.value.max()
        self.btc = padded_btc[(padded_btc.index >= self.start_date.date()) & (padded_btc.index <= self.end_date.date())]
        trimmed_data = padded_data[(padded_data.index >= self.start_date) & (padded_data.index <= self.end_date)]
        series_with_too_many_nas = self.finding_series_with_too_many_nas(padded_data)
        self.data = trimmed_data.drop(series_with_too_many_nas, axis=1)

    def prep_frame(self):
        ''' This method should be run each time you want to test a new start date '''
        # backfill missing data
        self.reduce_raw_data_series()
        self.ibtc = (self.btc / self.btc.iloc[0])['value']
        self.idata = self.data / self.data.iloc[0,]
        self.pdata = self.data.apply(pd.Series.pct_change)
        self.pbtc = self.btc.pct_change()


    def std_devs(self, index=False, diff=False):
        if index:
            std_devs = self.idata.apply(np.std)
            btc_std_dev = self.ibtc.std()
        else:
            std_devs = self.df.apply(np.std)
            btc_std_dev = self.btc.std()[0]
        if diff:
            return std_devs - btc_std_dev
        else:
            return btc_std_dev, std_devs

    def algorithm(self):
        ''' This method applies the matching algorithm, and returns the source and code
            of the winning series. '''

        diffs = self.std_devs(index=True, diff=True)
        fifty_closest_std_index = diffs.abs().order()[:50].index    
        fifty_closest_std_series = self.idata[fifty_closest_std_index]

        btc_pct = self.ibtc[-1] - self.ibtc[0]
        data_pct = fifty_closest_std_series.iloc[-1,:] - fifty_closest_std_series.iloc[0,:]
        ten_closest_pct_diffs_index = (data_pct - btc_pct).abs().order()[:10].index
        ten_closest_pct_diffs_series = fifty_closest_std_series[ten_closest_pct_diffs_index]

        series_diffs = (ten_closest_pct_diffs_series - btc_pct)
        least_squared_diff = (series_diffs ** 2).apply(np.sum).order().index[0]
        return least_squared_diff

    def match(self):
        self.prep_frame()
        match = self.algorithm()
        print self.start_date.date().isoformat(), self.end_date.date().isoformat(), match
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
        engine = self.session.bind  
        query = '''select * from cust_series;'''      
        # find bitcoin series id
        btc_id = self.session.query(EconomicMetadata.id).filter(EconomicMetadata.quandl_code==None).one()[0]
        data =  pd.read_sql_query(query, self.engine)
        btc = data[data.series_id==btc_id] # pull bitcoin data out of data frame into its own
        self.raw_btc = btc[['date', 'value']].set_index('date')
        data = data[data.series_id != btc_id]
        self.raw_data = data.pivot(index='date', columns='series_id', values='value')
        return btc, data

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
        for start, end in self.get_date_pairs():
#            if (self.raw_btc.index.max() < date): # are we matching a date we don't have?
#                return self.matches
            matching_series = self.get_series_to_match(start, end)
            algo = MatchingAlgorithm(start, end, self, matching_series)
            match = algo.match()
            if batch:
                self.matches.append(match)
            else:
                self.write_individual_match_to_db(match)

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
        self.match_days()
        if batch:
            self.write_matches_to_db()
        


if __name__ == '__main__':

    start_time = time.time()
    m = Matcher()
    m.run_matcher()

    '''
    m.run_matcher()
    end_time = time.time()
    minutes = (end_time - start_time) / 60
    print "\nMatcher Duration: {mins} minutes".format(mins=minutes)
    '''