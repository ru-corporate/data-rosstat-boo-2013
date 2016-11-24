# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 03:19:05 2016

@author: Евгений
"""
from itertools import islice
from rows import emit_raw_rows, emit_rows, get_colnames, COLUMNS, RENAMER, K, make_tuples 
q = next(islice(emit_raw_rows(2013),0,1))
w = dict(zip(COLUMNS,q))
assert w['1600'] == w['1700']
e = {RENAMER[k]:v for k, v in w.items() if k in RENAMER.keys()}
assert e['ta'] == e['tp']

data=make_tuples(q)[1]
cols=make_tuples(COLUMNS)[1]
ix = [COLUMNS.index(k) for k in RENAMER.keys()]
r=[COLUMNS[i] for i in ix]
assert r == list(RENAMER.keys())
t = next(islice(emit_rows(2013),0,1))
y = dict(zip(get_colnames(),t))
assert y['ta'] == y['tp']
