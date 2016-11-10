# -*- coding: utf-8 -*-
"""Create reduced datasets based on *G2013_ready.csv*"""

import pandas as pd
file_param = dict(sep=";", index=False, encoding="utf-8")

from column_names import rename_dict
df = pd.read_csv("data/G2013_ready.csv", sep = ";", 
                  dtype={'inn':str, 'region':str, 
                         'ok1': int, 'ok2': int, 'ok3':int})
cols =  ['year', 'title', 'inn', 'ok1', 'ok2', 'ok3', 'region'] + \
        list(rename_dict.keys())
df = df[cols].rename(columns=rename_dict)
df.to_csv("data/G2013_merged.csv", **file_param) 


def flags(df):
    # активы = пассивы
    flag1 = df.ta-df.tp
    # внеоборотные активы + оборотные активы = активы    
    flag2 = df.ta_fix + df.ta_nonfix - df.ta
    # капитал + долгосрочные обязательства + краткосрочные обязательства = всего пассивы
    flag3 = df.tp_cap+df.tp_short+df.tp_long-df.tp
    return flag1, flag2, flag3

# проверка АвтоВАЗ
vaz = df[df.inn == 6320002223]
assert (vaz.sales == 175152000).all()

# уменьшаем размер выборки
ix = (df.ta > 10000) | (df.sales> 12*5000)
subset1 = df[ix]
# все исходные данные в тыс. рублей, преобразрованные - млн руб.
data_cols = list(rename_dict.values())
subset1[data_cols]=(subset1[data_cols] / 1000).round(1)
subset1.to_csv("data/main.csv", **file_param) 

# предприятия с выручкой свыше 1 млрд. руб. 
BLN = 10**3
bln = subset1[subset1.sales>BLN]
bln.to_csv("data/bln.csv", **file_param)
bln.to_csv("data/bln.xlsx", **file_param)  