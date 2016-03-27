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
btcDf = pd.DataFrame.from_csv('btc.csv')
otherDf = pd.DataFrame.from_csv('data.csv')


# running through different windows produces a lot of NaN values in rollingDeviations...this effs up np.divide later
for _ in xrange(50):
	STARTINDEX = np.random.randint(100)
	LENGTH = np.random.randint(500)

	btcValues = btcDf.ix[btcDf.index[STARTINDEX:(STARTINDEX + LENGTH)]].values.reshape((-1,))
	meanBtcValues -= btcValues.mean()
	btcStandardDev = meanBtcValues.std()

	# Because numpy.correlate only supports single-dim arrays, we have to iterate
	# Or I need to spend more time reading the docs. One of those two.

	otherValues = otherDf.values
	otherValues = standardize(otherValues)

	rollingDeviations = pd.rolling_std(otherDf, LENGTH)#.values[(LENGTH - 1):, :]

	print rollingDeviations.shape
	print rollingDeviations.apply(pd.Series.isnull).apply(pd.Series.value_counts).iloc[:,0]

correlationsList = []

for colIdx in xrange(otherValues.shape[1]):
    print len(correlationsList), otherValues.shape[1]
    corr = np.correlate(otherValues[:, colIdx].reshape((-1,)), btcValues)
    correlationsList.append(corr)

corrArray = np.array(correlationsList)
print 'corrArray'
corrArray = corrArray.transpose()
print 'transposed'
# Because np.correlate is unnormalized, we can haz fix it
corrArray = np.divide(corrArray, rollingDeviations * btcStandardDev)
print 'divided'
# For reasons that evade me, your data have some series that don't vary for a 60-day period
# This makes for a zero denominator in the correlation equation. No can haz.
# Instead of intelligent exception handling, I'm just removing them.
# Really, this should probably happen BEFORE we divide by zero, but it's late and I workout early, so we're getting pretty hacky here.
corrArray = np.where(corrArray != float('inf'), corrArray, 0)

rawIdx = corrArray.argmax() # The max across the entire correlations array
cols = corrArray.shape[1]
rowIdx, colIdx = int(floor(rawIdx / cols)), rawIdx % cols # This will give us the index of the beginning of the maximally correlated sub-signal. Or something.
print 'something'
# Uncomment this line if you'd like to print the column index, from which you can get the asset Id
# print rawIdx, rowIdx, colIdx

otherSeries = otherValues[rowIdx:(rowIdx + LENGTH),colIdx].reshape((-1,))
