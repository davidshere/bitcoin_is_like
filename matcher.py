import pandas as pd 
import numpy as np
from fetcher import Fetcher


class Matcher(object):
    '''
        Class should take two inputs - a data frame and a series, both with date indices.
        Should prepare data and run series matching algorithms, producing a closest match.


        We should also make sure there's not SO much missing data in the time frames 
        we're looking at.

    '''

    def __init__(self, btc_series, df):
        self.raw_btc_series = btc_series
        self.raw_df = df
    
    def prep_frames(self, start_date):
        ''' This method should be run each time you want to test a new start date '''
        # backfill missing data
        processed_btc = self.raw_btc_series.fillna(method='pad')
        processed_df = self.raw_df.fillna(method='pad')
        
        # cut off data to the given start date
        self.btc = processed_btc[processed_btc.index >= start_date]
        self.comparison_series = processed_df[processed_df.index >= start_date]

    def covariance(self):
        covariances = dict()
        for i in data: # build a dict matching 
            covariances.update({i: m.btc.btc.cov(data[i])})
        return covariances

    def std_devs(self, diff=False):
        std_devs = self.comparison_series.apply(np.std)
        btc_std_dev = self.btc.apply(np.std)
        if diff:
            return std_devs - btc_std_dev
        else:
            return btc_std_dev, std_devs

    def total_percent_change(self, diff=False):
        comparison_series_pct_change = self.comparison_series.iloc[-1,:] / self.comparison_series.iloc[0,:]
        btc_pct_change = (self.btc.iloc[-1] / self.btc.iloc[0])[0]
        if diff:
            return comparison_series_pct_change - btc_pct_change
        else:
            return btc_pct_change, comparison_series_pct_change

    def max_percent_change(self):
        max_btc = self.btc.max() / self.btc.min()
        values = {}
        for i in data:
            series = data[i]
            values.update({i:(series.max()/series.min())})
        return max_btc, pd.Series(values)

    def variances(self):
        devs = self.std_devs()
        return np.sqrt(devs[0]), np.sqrt(devs[1])



    def matcher(self):
        ''' This method applies the matching algorithm, and returns the source and code
            of the winning series.'''
        pass

if __name__ == '__main__':

    data = pd.read_csv('sample_data/bnp_fx_series.csv')
    data.set_index('Date', inplace=True)

    START_DATE = '2014-03-06'
    btc = Fetcher().fetch_bitcoin_series()

    m = Matcher(btc, data)
    m.prep_frames(START_DATE)
