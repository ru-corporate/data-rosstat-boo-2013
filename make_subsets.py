# -*- coding: utf-8 -*-

import pandas as pd
from slicer import NON_MONETARY_COLUMNS

# читаем данные
df = pd.read_csv('data/merged.csv',sep=";") 
#df.set_index('inn')
data_cols = [x for x in df.columns.tolist() if x not in NON_MONETARY_COLUMNS]

# проверка АвтоВАЗ
vaz = df[df.inn == 6320002223]
assert (vaz.sales == 175152000).all()

# уменьшаем размер выборки
ix = (df.ta > 10000) | (df.sales> 12*5000)
subset = df[ix]
# все исходные данные в тыс. рублей, преобразрованные - млн руб.
subset[data_cols] = (subset[data_cols] / 1000).round(1)  # todo - do this earlier
#subset.to_csv("data/main.csv", sep = ";", encoding = "utf-8")

# предприятия с выручкой свыше 1 млрд. руб. 
BLN = 10**3
a = subset[subset.sales>BLN]
a.to_csv("data/main.csv", sep = ";", encoding = "utf-8")
#a.to_excel("data/bln.xlsx")
