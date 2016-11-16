# -*- coding: utf-8 -*-
"""
Common functions used inother modules.
"""

import time
##common timer function
def print_elapsed_time(f):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = f(*args, **kwargs)       
        print("Time elapsed: %.1f seconds" % (time.time()-start_time))         
        return result
    return wrapper

