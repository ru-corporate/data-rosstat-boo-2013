# -*- coding: utf-8 -*-
"""Testing reader.py."""

from itertools import islice
from reader import emit_rows, emit_dicts, emit_raw_rows, emit_raw_dicts
from reader import inn_mask, emit_rows_by_inn
from reader import Subset, Dataset
#todo: add testing Dataset

def test_subset():
     ITEMS = ['77']
     Subset(2015, 'test1').include(ITEMS)._inc == ITEMS
     Subset(2015, 'test1').exclude(ITEMS)._exc == ITEMS

#
#   row/dict emitters
#

def test_inn_filter():
    gen = [{'inn': 0}, {'inn': 10}, {'inn': 20}, {'inn': 30}]
    incs = [0,10]
    exs = [20,30]
    
    assert [] == list(inn_mask([10],[10]).apply(gen))     
    assert [{'inn': 0}] == list(inn_mask([0, 10],[10]).apply(gen))  
    
    a = list(inn_mask(incs,exs).apply(gen))     
    b = list(inn_mask(incs).apply(gen))
    c = list(inn_mask(None,exs).apply(gen))   
    assert a == b
    assert b == c
    


def test_emitters():
    assert next(emit_rows(2015)) == [2015, '20160624', 28, 11, 0, 'Открытое акционерное общество', 'Энерготехмаш', '34', '3435900517', '00110467', '10000', '16', '384', '23616', '47666', '124323', '171989', '223076', '33574', '-250123', '227579', '194533', '171989', '39311', '-49052', '29000', '-229430', '27492', '42114', '347639', '389753', '-32497', '223076', '233014', '4806', '189236', '389753', '335342', '-30226', '25572', '-62270', '26572', '16333', '3123', '23721', '0', '0', '0', '0']
                                      
    assert next(emit_rows(2015)) == Dataset(2015).nth(0)   
    
# def test_inn_list():

    # _ = [int(x) for x in DatasetByINN(2015).inn_list if not str(x).startswith("#")]

def test_inn_slicing():
    z = next(emit_rows(2015))
    inn = z[8]
    assert inn == '3435900517'
    assert next(emit_rows(2015)) == next(emit_rows_by_inn(2015, include=[inn], exclude=[]))

def test_a_p():
    gen1=emit_raw_dicts(2015)
    gen2=emit_dicts(2015)
    m = next(gen1)
    assert m['1600'] == '171989'
    assert m['1700'] == '171989'

    u = next(gen2)
    assert u['ta'] == '171989'
    assert int(u['ta'])-int(u['ta_fix'])-int(u['ta_nonfix']) == 0


def test_emitters2():

    TEST_YEAR = 2015
    POS = 0

    def getter(func, n=POS, year=TEST_YEAR):
        return next(islice(func(year),n,n+1))

    raw_row = getter(emit_raw_rows)
    raw_dict = getter(emit_raw_dicts)
    assert raw_dict['1600'] == raw_dict['1700']

    parsed_row = getter(emit_rows)
    parsed_dict = getter(emit_dicts)

    d=parsed_dict
    assert int(d['ta']) == int(d['ta_fix'])+int(d['ta_nonfix'])
    assert int(d['tp']) == int(d['tp_cap'])+int(d['tp_short'])+int(d['tp_long'])
    assert int(d['ta']) == int(d['tp'])


