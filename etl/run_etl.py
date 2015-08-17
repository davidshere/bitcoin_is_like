import time

from etl.fetcher import Fetcher
from etl.matcher import Matcher

def run():
	fetcher_start_time = time.time()
	f = Fetcher()
	f.update()
	fetcher_end_time = time.time()
	fetcher_minutes = (fetcher_start_time - fetcher_end_time)

	matcher_start_time = time.time()
	m = Matcher()
	m.run_matcher()	
	matcher_end_time = time.time()
	matcher_minutes = (matcher_start_time - matcher_end_time)
	return {'fetcher_duration': fetcher_minutes, 'matcher_duration': matcher_minutes}

if __name__ == '__main__':

	etl_start_time = time.time()
	run_times = run() # run the etl, grab the dict with the durations
	etl_end_time = etl_time.time()
	etl_minutes = (start_time - end_time) / 60
	print 'Fetcher Duration: {mins} minutes'.format(mins=run_times['fetcher_duration'])
	print 'Matcher Duration: {mins} minutes'.format(mins=run_times['matcher_duration'])
	print 'ETL Duration: {mins} minutes'.format(mins=etl_minutes)

