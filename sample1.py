# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 14:26:25 2016

@author: Евгений
"""
from reader import Dataset

df15=Dataset(2015).read_df()
df13=Dataset(2013).read_df()

def get(df):
   z = df[df.unit==385][['title','inn','ta']]
   #z['ta2'] = z['ta'] * 1000
   return z.sort_values('ta')[['title','inn','ta']]
   
z = get(df15)
w = get(df13)

c = ['unit_x', 'unit_y']
df = z.merge(w, on='inn', how = 'left')

ex1 = df.ta_x!=df.ta_y
print("# Exclusions 1 (values do not change between 2015 and 2013):")
print("ex1 =", df[~ex1].inn.tolist())

q = df[ex1].sort_values('ta_x')[['title_x', 'ta_y', 'ta_x', 'inn']]
ex2=q.ta_y.isnull()
print("# Exclusions 2 (values not present in 2013, unit eq 385):")
print("ex2 =", q[ex2].inn.tolist())

UNIT385_EXCLUSIONS = ex1+ex2

