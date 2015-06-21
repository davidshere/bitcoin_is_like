from fetcher import Fetcher
from matcher import Matcher

if __name__ == '__main__':

	f = Fetcher()
	f.update()

	m = Matcher()
	m.run_matcher()