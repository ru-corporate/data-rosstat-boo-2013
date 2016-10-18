# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 17:02:23 2016

@author: Евгений
"""

import reader

def test_get_csv_lines():
   a = next(reader.get_csv_lines())
   assert isinstance(a['name'], str)  

tests = {   1: [1, None, None]
       , "01": [1, None, None]
    , "44.20": [44,20,None]
  , "1.10.11": [1, 10,  11]}

def test_okved():  
   for k, v in tests.items():
      assert reader.okved_tuple(k) == v  