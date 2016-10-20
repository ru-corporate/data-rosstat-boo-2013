# -*- coding: utf-8 -*-

import reader

def test_get_csv_lines():
   a = next(reader.get_csv_lines())
   assert isinstance(a['name'], str)  

def test_okved():  
    tests = {   1: [1, None, None]
           , "01": [1, None, None]
        , "44.20": [44,20,None]
      , "1.10.11": [1, 10,  11]}
    for k, v in tests.items():
        assert reader._okved_tuple(k) == v  