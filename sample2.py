# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 15:36:24 2016

@author: Евгений
"""

from reader import Dataset
df = Dataset(2015).read_df()
df0 = Dataset(2013).read_df()

df.nlargest(20, 'ta')[['title','inn', 'unit']]

# suspicious
df[(df.tp_cap == df.tp) & (df.ta==df.ta_fix)][['title','ta']].sort_values('ta')


#other suspicious inns
bad_inns = ['7702844336', '7710244903', '7707089648', '7707322083']

df[df.inn.isin(bad_inns)].transpose()

#7707089648
#http://kommersant.ru/doc/125895