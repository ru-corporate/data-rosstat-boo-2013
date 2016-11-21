# -*- coding: utf-8 -*-


import time

def print_elapsed_time(f):
    """Print execution time for *f* to screen."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = f(*args, **kwargs)       
        print("Time elapsed: %.1f seconds" % (time.time()-start_time))         
        return result
    return wrapper

#     
# def deny_if_exists(f)  


 # make_adjusted_csv(self, overwrite=False):
    # file_exists = os.path.exists(self.adjusted_csv)
    # if file_exists and overwrite is False:
        # print("Adjusted CSV already exists:", self.adjusted_csv)
    # if not file_exists or overwrite is True:
        # print("Year:", self.year)
        # print("Writing adjusted CSV:", self.adjusted_csv)            
        # #
        # gen = self.parsed_rows()
        # csv_access.to_csv(self.adjusted_csv, gen, self.columns)
        # #
    # return self.adjusted_csv # success code     
