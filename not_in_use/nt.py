# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 21:16:32 2016

@author: Евгений
"""
import time

def print_elapsed_time(foo):
    """Print execution time for *f* to screen."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = foo(*args, **kwargs)
        print("Time elapsed: %.2f seconds" % (time.time()-start_time))
        return result
    return wrapper 

N = 100*100*100

from collections import namedtuple, OrderedDict

cols = ['a', 'b']
gen = [[0,1], [20,50]] * N

Row = namedtuple("Row", cols)
gen1 = map(Row._make, gen)

#as_od = lambda vals: OrderedDict(zip(cols,vals))
#gen1 = map(as_od, gen)

#1. apply filter to kill rows not supported by reader
#1.1. filter out no INN (optional)
#2. emit_named_tuple 

@print_elapsed_time
def od(gen):
    as_od = lambda vals: OrderedDict(zip(cols,vals))
    gen1 = map(as_od, gen)
    return list(gen1) 

@print_elapsed_time
def nt(gen):
    Row = namedtuple("Row", cols)
    return map(Row._make, gen)
    
o = od(gen)
n = nt(gen)
     





#fields = ("permalink","company","numEmps", "category","city","state","fundedDate", "raisedAmt","raisedCurrency","round")
#FundingRecord = namedtuple('FundingRecord', fields)

#def read_funding_data(path):
#    with open(path, 'rU') as data:
#        data.readline()            # Skip the header
#        reader = csv.reader(data)  # Create a regular tuple reader
#        for row in map(FundingRecord._make, reader):
#            yield row
#
#if __name__ == "__main__":
#    for row in read_funding_data(FUNDING):
#        print row
#        break

