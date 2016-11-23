# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 01:16:49 2016

@author: Евгений
"""

it = iter(range(1000000))

class Progress():
    """Minimal progress spinner remotely inspired by 
       http://docs.astropy.org/en/v0.2/_generated/astropy.utils.console.Spinner.html#"""
    
    STEP = 100*1000
    
    def __init__(self):
        self.count=0 
        self.k=0 
           
    def next(self):
        self.count += 1
        self.k += 1
        if self.k == self.STEP:
            print(self.count)
            self.k = 0
            
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        pass
            

def pipe(gen):
    with Progress() as prog:
        for item in gen:
            prog.next()
            yield item

            
list(pipe(it))            
            