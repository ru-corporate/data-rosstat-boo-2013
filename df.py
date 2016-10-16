# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 02:07:11 2016

@author: Евгений
"""

import pandas as pd
z = pd.read_csv("_all2013.csv", sep = ";")
d = z[['inn','okved','title','name','_2110','_1410']]
s = d[d['_1410']>0].sort_values('okved')
s.to_csv("with_debt.csv",sep=";", index=False)
s.to_excel("with_debt.xlsx")