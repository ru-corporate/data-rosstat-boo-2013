# -*- coding: utf-8 -*-

from list_reader import dequote, okved3, EMPTY, parse_colnames, parse_row, csv_block

def test_lines():
   a = next(csv_block(1))
   assert isinstance(a, list)
   assert isinstance(a[5], str)   

def test_okved():  
    tests = { "1": [1, EMPTY, EMPTY]
           , "01": [1, EMPTY, EMPTY]
        , "44.20": [44,  20,  EMPTY]
      , "1.10.11": [1,   10,   11]}
    for k, v in tests.items():
        assert okved3(k) == v            
        
def test_dequote():
    assert dequote('Открытое акционерное общество "База отдыха "Энергетик"') == \
           ['Открытое акционерное общество', 'База отдыха "Энергетик"']
    assert dequote('Общество с ограниченной ответственностью "РИОНИ"') == \
           ['Общество с ограниченной ответственностью', 'РИОНИ']
    assert dequote('МУНИЦИПАЛЬНОЕ УНИТАРНОЕ ПРЕДПРИЯТИЕ "ТЕХНО-ТОРГОВЫЙ ЦЕНТР "РЕМБЫТТЕХНИКА" МУНИЦИПАЛЬНОГО ОБРАЗОВАНИЯ "ГОРОД АРХАНГЕЛЬСК"') == ['МУНИЦИПАЛЬНОЕ УНИТАРНОЕ ПРЕДПРИЯТИЕ', 'ТЕХНО-ТОРГОВЫЙ ЦЕНТР "РЕМБЫТТЕХНИКА" МУНИЦИПАЛЬНОГО ОБРАЗОВАНИЯ "ГОРОД АРХАНГЕЛЬСК"']

def test_parse_row():    
    cols = parse_colnames()
    r = parse_row(vec=next(csv_block(1)))
    assert len(r) == 271
    d = dict(zip(cols, r))
    assert d['year'] > 2000
    assert isinstance(d['title'],str)   