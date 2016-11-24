# -*- coding: utf-8 -*-
"""Testing modules."""

from rows import dequote, okved3, EMPTY
from rows import emit_rows, emit_dicts, emit_raw_rows, emit_raw_dicts, __full_transform__
from itertools import islice
    
#
#   stateless transformation functions
#

def test_okved():  
    tests = { "1": [1, EMPTY, EMPTY]
           , "01": [1, EMPTY, EMPTY]
        , "44.20": [44,   20, EMPTY]
      , "1.10.11": [1,    10,    11]}
    for k, v in tests.items():
        assert okved3(k) == v            
        
def test_dequote():
    tests = {'Открытое акционерное общество "База отдыха "Энергетик"': ('Открытое акционерное общество', 'База отдыха "Энергетик"')
           , 'Общество с ограниченной ответственностью "РИОНИ"': ('Общество с ограниченной ответственностью', 'РИОНИ')
           , 'МУНИЦИПАЛЬНОЕ УНИТАРНОЕ ПРЕДПРИЯТИЕ "ТЕХНО-ТОРГОВЫЙ ЦЕНТР "РЕМБЫТТЕХНИКА" МУНИЦИПАЛЬНОГО ОБРАЗОВАНИЯ "ГОРОД АРХАНГЕЛЬСК"': 
           ('МУНИЦИПАЛЬНОЕ УНИТАРНОЕ ПРЕДПРИЯТИЕ', 'ТЕХНО-ТОРГОВЫЙ ЦЕНТР "РЕМБЫТТЕХНИКА" МУНИЦИПАЛЬНОГО ОБРАЗОВАНИЯ "ГОРОД АРХАНГЕЛЬСК"')
           }
    for k, v in tests.items():
        assert dequote(k) == v  
        
#
#   row/dict emitters
#
   
def test_emitters():
    assert next(emit_rows(2015)) ==  [2015, '20160624', 28, 11, 0, 'Открытое акционерное общество', 'Энерготехмаш', '34', 
                                      '3435900517', '00110467', '10000', '16', 23616, 47666, 124323, 171989, 223076, 
                                      33574, -250123, 227579, 194533, 171989, 39311, -49052, 29000, -229430, 27492, 
                                      42114, 347639, 389753, -32497, 223076, 233014, 4806, 189236, 389753, 335342, -30226, 
                                      25572, -62270, 26572, 16333, 3123, 23721, 0, 0, 0, 0]                                      
    assert next(emit_rows(2015)) == next(emit_rows(2015, ['3435900517']))

def test_a_p():
    gen1=emit_raw_dicts(2015)
    gen2=emit_dicts(2015, ['3435900517'])
    m = next(gen1)
    assert m['1600'] == '171989'
    assert m['1700'] == '171989'
    
    u = next(gen2)
    assert u['ta'] == 171989
    assert u['ta']-u['ta_fix']-u['ta_nonfix'] == 0
    
    
def test_emitters2():

    TEST_YEAR = 2015
    POS = 0 
    
    def getter(func, n=0, year=TEST_YEAR):
        return next(islice(func(year),n,n+1))       
    
    raw_row = getter(emit_raw_rows,POS)
    raw_dict = getter(emit_raw_dicts,POS)
    assert raw_dict['1600'] == raw_dict['1700']
    
    parsed_row = getter(emit_rows,POS)
    parsed_dict = getter(emit_dicts,POS)
    
    d=parsed_dict
    assert d['ta'] == d['ta_fix']+d['ta_nonfix']
    assert d['tp'] == d['tp_cap']+d['tp_short']+d['tp_long']
    assert d['ta'] == d['tp'] 
    
    assert parsed_dict == next(__full_transform__(iter([raw_row]), year=TEST_YEAR))
    
