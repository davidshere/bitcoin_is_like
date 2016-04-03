'''
Created on Dec 27, 2015
@author: SeanEaster
'''

from math import floor

import numpy as np
import pandas as pd

def standardize(array):

	columnMeans = array.mean(axis=0)
	return array - columnMeans

# First, load the btc data and other assets
btc_df = pd.DataFrame.from_csv('btc.csv')
other_df = pd.DataFrame.from_csv('data.csv')

# Now, given a start date, we want to find the max-correlated series of the same length
# Try changing these to try different start dates and period windows.

STARTINDEX = 11
LENGTH = 90

btc_values = btc_df.ix[btc_df.index[STARTINDEX:(STARTINDEX + LENGTH)]].values.reshape((-1,)) 
btc_values -= btc_values.mean()
btcStandardDev = btc_values.std()

# Because numpy.correlate only supports single-dim arrays, we have to iterate
# Or I need to spend more time reading the docs. One of those two.

other_values = other_df.values
other_values = standardize(other_values)
rollingDeviations = pd.rolling_std(other_df, LENGTH).values[(LENGTH - 1):,:]

corr_list = []
for colIdx in range(other_values.shape[1]):
    corr = np.correlate(other_values[:,colIdx].reshape((-1,)), btc_values)
    corr_list.append(corr)

corr_array = np.array(corr_list)
corr_array = corr_array.transpose()

# Because np.correlate is unnormalized, we can haz fix it
corr_array = np.divide(corr_array, rollingDeviations * btcStandardDev)

# For reasons that evade me, your data have some series that don't vary for a 60-day period
# This makes for a zero denominator in the correlation equation. No can haz.
# Instead of intelligent exception handling, I'm just removing them.
# Really, this should probably happen BEFORE we divide by zero, but it's late and I workout early, so we're getting pretty hacky here.
corr_array = np.where(corr_array != float('inf'), corr_array, 0)


rawIdx = corr_array.argmax() # The max across the entire correlations array
cols = corr_array.shape[1]
rowIdx, colIdx = int(floor(rawIdx / cols)), rawIdx % cols # This will give us the index of the beginning of the maximally correlated sub-signal. Or something.

# Uncomment this line if you'd like to print the column index, from which you can get the asset Id
#print rawIdx, rowIdx, colIdx




