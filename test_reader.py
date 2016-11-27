# -*- coding: utf-8 -*-
"""Testing modules."""

from itertools import islice
from reader import emit_rows, emit_dicts, emit_raw_rows, emit_raw_dicts
from reader import filter_by_inn, emit_rows_by_inn
from reader import DatasetByINN

#
#   row/dict emitters
#

def test_inn_filter():
    assert 1 == next(filter_by_inn(iter([{'inn':1}]), [1,2]))['inn']


def test_emitters():
    assert next(emit_rows(2015)) ==  [2015, '20160624', 28, 11, 0, 'Открытое акционерное общество', 'Энерготехмаш', '34',
                                      '3435900517', '00110467', '10000', '16', 23616, 47666, 124323, 171989, 223076,
                                      33574, -250123, 227579, 194533, 171989, 39311, -49052, 29000, -229430, 27492,
                                      42114, 347639, 389753, -32497, 223076, 233014, 4806, 189236, 389753, 335342, -30226,
                                      25572, -62270, 26572, 16333, 3123, 23721, 0, 0, 0, 0]
def test_inn_list():

    _ = [int(x) for x in DatasetByINN(2015).inn_list if not str(x).startswith("#")]

def test_inn_slicing():
    z = next(emit_rows(2015))
    inn = z[8]
    assert inn == '3435900517'
    assert next(emit_rows(2015)) == next(emit_rows_by_inn(2015, inn_list=[inn]))

def test_a_p():
    gen1=emit_raw_dicts(2015)
    gen2=emit_dicts(2015)
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



##for i,pd in enumerate(islice(emit_parsed_dicts(2015),0,1)):
##    if pd['cash_out_investment_of']>500*1000:
##        pass
#        #print(i, "%12d" % pd['cash_out_investment_of'], pd['region'], pd ['title'])
#

#
#is_valid([1])
