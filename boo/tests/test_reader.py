# -*- coding: utf-8 -*-
"""Testing reader.py."""

from itertools import islice
from reader import emit_rows, emit_dicts, emit_raw_rows, emit_raw_dicts
from reader import Dataset
from subset import emit_rows_by_inn
#todo: add testing Dataset


#
#   row/dict emitters
#


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
    m = next(gen1)
    assert m['1600'] == '171989'
    assert m['1700'] == '171989'
    assert m['1600_lag'] == m['1700_lag']
    
    gen2=emit_dicts(2015)
    u = next(gen2)
    assert u['ta'] == '171989'
    assert int(u['ta'])-int(u['ta_fix'])-int(u['ta_nonfix']) == 0
    assert int(u['ta_lag'])-int(u['ta_fix_lag'])-int(u['ta_nonfix_lag']) == 0


def test_emitters2():

    TEST_YEAR = 2015
    POS = 25

    def getter(func, n=POS, year=TEST_YEAR):
        return next(islice(func(year),n,n+1))

    raw_dict = getter(emit_raw_dicts)
    assert raw_dict['1600'] == raw_dict['1700']
    assert raw_dict['1600_lag'] == raw_dict['1700_lag']

    d = getter(emit_dicts)
    assert int(d['ta']) == int(d['ta_fix'])+int(d['ta_nonfix'])
    assert int(d['tp']) == int(d['tp_cap'])+int(d['tp_short'])+int(d['tp_long'])
    assert int(d['ta']) == int(d['tp'])
    assert int(d['ta_lag']) == int(d['tp_lag'])

