from datetime import datetime, timedelta
import time

import numpy as np
import pandas as pd 
from sqlalchemy import func

import connection_manager as cm
from models import EconomicMetadata, EconomicSeries, Match

class MatchingAlgorithm(object):

    def __init__(self, start_date, matching_object):
        self.start_date = pd.to_datetime(start_date)
        self.matching_object = matching_object

    def prep_frame(self):
        ''' This method should be run each time you want to test a new start date '''
        # backfill missing data
        padded_btc = self.matching_object.raw_btc.fillna(method='pad')
        padded_data = self.matching_object.raw_data.fillna(method='pad')

        self.btc = padded_btc[padded_btc.index >= self.start_date]
        self.data = padded_data[padded_data.index >= self.start_date]
        self.ibtc = (self.btc / self.btc.iloc[0])['value']
        self.idata = self.data / self.data.iloc[0,]

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
        print self.start_date, match
        return {'start_date': self.start_date, 'match':match}


class Matcher(object):
    ''' Should mirror Fetcher in that it reads from a table, determines 
        which dates are missing, and fills in the matches for the missing
        dates. Uses a MatchingAlgorithm object to identify matches. '''

    def __init__(self):
        self.session = cm.DBConnect().create_session()
        self.list_of_dates = []
        self.matches = []

    def load_data(self):
        engine = self.session.bind        
        # find bitcoin series id
        btc_id = self.session.query(EconomicMetadata.id).filter(EconomicMetadata.quandl_code==None).one()[0]
        data = pd.read_sql_table('cust_series', engine) # fetch data
        btc = data[data.series_id==btc_id] # pull bitcoin data out of data frame into its own
        self.raw_btc = data[['date', 'value']].set_index('date')
        data = data[data.series_id != btc_id]
        self.raw_data = data.pivot(index='date', columns='series_id', values='value')
        return btc, data

    def generate_dates_to_be_matched(self):
        '''
            V1: Generate a list of dates, match all of them
            V2: Generate a list of date pairs, remove ones that exist in the table, match the rest
        '''
        start = self.session.query(func.min(EconomicSeries.date)).one()[0] # fetch earliest date
        end = datetime.now().date() - timedelta(days = 15)
        number_of_days = (end - start).days
        list_of_dates = list()
        for i in range(number_of_days):
            next_day = start + timedelta(days = i)
            next_day = pd.to_datetime(next_day)
            self.list_of_dates.append(next_day)

    def generate_match_pairs(self):
        ''' Should produce a list of tuples containing start and end dates to be matched '''
        pass

    def match_days(self):
        for date in self.list_of_dates:
            if (self.raw_btc.index.max() < date) or (self.raw_data.index.max() < date): # are we matching a date we don't have?
                return self.matches
            algo = MatchingAlgorithm(date, self)
            match = algo.match()
            self.matches.append(match)

    def write_matches_to_db(self):
        matches = [Match(start_date = datapoint['start_date'],
                         series_id=datapoint['match'])
                         for datapoint in self.matches]
        self.session.add_all(matches)
        self.session.commit()
        self.session.close()
        return 0

    def run_matcher(self):
        ''' 1) Loads raw data from the DB, 
            2) Puts together a set of dates, 
            3) Runs the matching algorithm
            4) Writes matches to DB '''

        self.load_data() 
        self.generate_dates_to_be_matched()
        self.match_days()
        self.write_matches_to_db()
        


if __name__ == '__main__':

    start_time = time.time()
    m = Matcher()
    m.run_matcher()
    end_time = time.time()
    minutes = (start_time - end_time) / 60
    print "Matcher Duration: {mins} minutes".format(mins=minutes)