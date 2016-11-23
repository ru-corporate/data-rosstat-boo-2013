# -*- coding: utf-8 -*-

import time
import config

def print_elapsed_time(f):
    """Print execution time for *f* to screen."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = f(*args, **kwargs)       
        print("Time elapsed: %.1f seconds" % (time.time()-start_time))         
        return result
    return wrapper

    
class GenericLogger():
    """Log errors while reading rows."""
    def __init__(self, filename):
        self.log_filename=filename 
        
    def start(self):
        with open(self.log_filename,'w') as f:
            print("Log started", file=f)
            
    def report(self, *msg):        
        with open(self.log_filename,'a') as f:
            print(*msg)
            print(*msg, file=f)        

class Logger(GenericLogger):

    def __init__(self, year):
        filename = config.make_path_error_log(year)
        super().__init__(filename)
        