# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 02:16:57 2016

@author: Евгений
"""

if __name__ == "__main__":
    gen = [{'inn': 0}, {'inn': 10}, {'inn': 20}, {'inn': 30}]
    incs = [0,10]
    exs = [20,30]
    
    z = list(filter_by_inn(gen, include_inns=incs))
    assert incs ==  [0,10]
    x = list(filter_by_inn(gen, exclude_inns=exs))
    assert z == x   
    
    assert [] == list(inn_mask([10],[10]).apply(gen))     
    assert [{'inn': 0}] == list(inn_mask([0, 10],[10]).apply(gen))  
    
    a = list(inn_mask(incs,exs).apply(gen))     
    b = list(inn_mask(incs).apply(gen))
    c = list(inn_mask(None,exs).apply(gen))   
    assert a == b
    assert b == c
    
    d = list(filter_by_inn(gen, include_inns=incs, exclude_inns=exs))
    assert a == d
    assert b == list(filter_by_inn(gen, incs))
    assert c == list(filter_by_inn(gen, None, exs)) 