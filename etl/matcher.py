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
    
    def prep_frame(self, start_date):
        ''' This method should be run each time you want to test a new start date '''

        self.start_date = start_date

        # backfill missing data
        processed_btc = self.raw_btc_series.fillna(method='pad')
        processed_df = self.raw_df.fillna(method='pad')
        
        # cut off data to the given start date
        self.btc = processed_btc[processed_btc.index >= start_date]
        self.comparison_series = processed_df[processed_df.index >= start_date]

        self.indexed_btc = self.btc / self.btc.iloc[0]
        self.indexed_series = self.comparison_series / self.comparison_series.iloc[0,]

    def index_series(self, series):
        return series / series[0]

    def covariance(self):
        covariances = dict()
        for i in self.comparison_series: # build a dict matching 
            covariances.update({i: self.btc.cov(self.comparison_series[i])})
        return covariances

    def std_devs(self, index=False, diff=False):
        if index:
            std_devs = self.indexed_series.apply(np.std)
            btc_std_dev = self.indexed_btc.std()[0] 
        else:
            btc_std_dev = self.btc.std()[0]
            std_devs = self.comparison_series.apply(np.std)
        if diff:
            return std_devs - btc_std_dev
        else:
            return btc_std_dev, std_devs

    def variances(self, index=False):
        standard_deviations = self.std_devs(index=index)
        return map(lambda x: x**2, standard_deviations)

    def total_percent_change(self, diff=False):
        comparison_series_pct_change = self.comparison_series.iloc[-1,:] / self.comparison_series.iloc[0,:]
        btc_pct_change = (self.btc.iloc[-1] / self.btc.iloc[0])[0]
        if diff:
            return comparison_series_pct_change - btc_pct_change
        else:
            return btc_pct_change, comparison_series_pct_change

    def max_percent_change(self):
        max_btc = (self.btc.max() / self.btc.min())[0]
        values = {}
        for i in self.comparison_series:
            series = self.comparison_series[i]
            values.update({i:(series.max()/series.min())})
        return max_btc, pd.Series(values)

    def mean_absolute_deviation(self):
        pass



    def matcher(self, algorithm=1):
        ''' This method applies the matching algorithm, and returns the source and code
            of the winning series.

            Maybe take rolling averages? 14-day moving averages?

            Idea 1:
                * First, find the series with the closest standard deviations
                * Of those, find the one with the closest percent change

            Idea 2:
                * Have a point system. So, if you're the closest in standard deviation, 
                  that's a certain number of points. If you're closest in percent change, that's
                  another couple of points
                * Add up the points, and the highest points wins. We can also weight different measures
                  differently, emphasizing one statistic over the other
        '''
        if algorithm == 1:
            diffs = self.std_devs(index=True, diff=True)
            fifty_closest_std = diffs.abs().order()[:50]            
            return fifty_closest_std.index[0]

if __name__ == '__main__':

    data = pd.read_csv('sample_data/bnp_fx_series.csv')
    data.set_index('Date', inplace=True)

    START_DATE = '2014-03-06'
    btc = pd.read_csv('sample_data/historical_btc_data.csv') #Fetcher().fetch_bitcoin_series()
    btc.set_index('date', inplace=True)

    m = Matcher(btc, data)
    m.prep_frame(START_DATE)

    a, b = m.variances()
