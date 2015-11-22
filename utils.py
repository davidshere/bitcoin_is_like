import time


def rate_limiter(max_per_second):
    '''
        This decorator function is meant to ensure that we don't run up against
        Quandl's limit of 20,000 calls in ten minutes. So, no more than 3 calls
        per second

        modified with gratitude from: 
        http://blog.gregburek.com/2011/12/05/Rate-limiting-with-decorators/

        TODO: futz with it so it's explicity on a ten minute time scale
    '''
    min_interval = 1.0 / float(max_per_second)
    def decorate(func):
        last_time_called = [0.0]
        def rate_limited_function(*args,**kargs):
            elapsed = time.clock() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait>0:
                time.sleep(left_to_wait)
            ret = func(*args,**kargs)
            last_time_called[0] = time.clock()
            return ret
        return rate_limited_function
    return decorate