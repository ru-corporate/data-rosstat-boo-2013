# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 03:17:12 2016

@author: Евгений
"""

import time

def print_elapsed_time(func):
    def wrapper(self):
        start_time = time.time()
        result = func(self, *args) 
        print("Time elapsed: %.1f seconds" % (time.time()-start_time)) 
        return result
    return wrapper

@print_elapsed_time
def foo():
    for i in range(10^6):
        i=i+1
        pass    
        


def dec_check(f):
  def deco(self):
    print ('In deco')
    f(self)
  return deco

class bar(object):
  @print_elapsed_time
  def foo(self, x):
    print ('in bar.foo')
    
bar().foo()    