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

        self.ibtc = (self.btc / self.btc.iloc[0])['price']
        self.idata = self.comparison_series / self.comparison_series.iloc[0,]

    def index_series(self, series):
        return series / series[0]

    def covariance(self):
        covariances = dict()
        for i in self.comparison_series: # build a dict matching 
            covariances.update({i: self.btc.cov(self.comparison_series[i])})
        return covariances


    def variances(self, index=False):
        standard_deviations = self.std_devs(index=index)
        return map(lambda x: x**2, standard_deviations)

    def total_percent_change(self, diff=False):
        comparison_series_pct_change = self.comparison_series.iloc[-1,:] / self.comparison_series.iloc[0,:]
        btc_pct_change = self.btc.iloc[-1] / self.btc.iloc[0]
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

    def cross_correlate(self):
        correlations = {i: np.correlate(self.idata[i], self.ibtc) for i in self.idata}
        return pd.DataFrame(correlations)

    def mean_absolute_deviation(self):
        pass

    def std_devs(self, index=False, diff=False):
        if index:
            std_devs = self.idata.apply(np.std)
            btc_std_dev = self.ibtc.std()
        else:
            std_devs = self.comparison_series.apply(np.std)
            btc_std_dev = self.btc.std()[0]
        if diff:
            return std_devs - btc_std_dev
        else:
            return btc_std_dev, std_devs

    def matcher(self):
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


if __name__ == '__main__':

    data = pd.read_csv('sample_data/full_test_frame.csv')
    data.set_index('Date', inplace=True)


    btc = pd.read_csv('sample_data/historical_btc_data.csv') #Fetcher().fetch_bitcoin_series()
    btc.set_index('date', inplace=True)

    START_DATE = '2014-01-01'
    m = Matcher(btc, data)
    m.prep_frame(START_DATE)

    match = m.matcher()
    print match

    correlations = m.cross_correlate()