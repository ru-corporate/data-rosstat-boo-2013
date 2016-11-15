# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 01:05:08 2016

@author: Евгений
"""


from config import get_local_path, VALID_YEARS, TEST_RAW_CSV
from remote import RemoteDataset
from row_parser import adjust_row
from reader import get_csv_lines

assert "data\\source\\csv\\123.csv" == get_local_path("123.csv", "clean_csv")

assert RemoteDataset(2012, silent=True).download().unrar()
assert RemoteDataset(2013, silent=True).download().unrar()
assert RemoteDataset(2014, silent=True).download().unrar()
assert RemoteDataset(2015, silent=True).download().unrar()
   
for f in [RemoteDataset(x).rar_content() for x in VALID_YEARS]:
   assert f  
   
assert len([adjust_row(x,2015) for x in get_csv_lines(TEST_RAW_CSV)]) == 5   