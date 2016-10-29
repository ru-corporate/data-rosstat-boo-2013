# -*- coding: utf-8 -*-
"""Create reduced datasets based on *all2013.csv* and *inn.csv*"""

import pandas as pd
import os

def save(df, filebase, folder=None):
    if folder:
        pathbase = os.path.join(folder, filebase)    
    else:
        pathbase = filebase
    df.to_csv(pathbase + ".csv", sep = ";", index = False, encoding = "utf-8")
    df.to_excel(pathbase + ".xlsx", index = False)


df = pd.read_csv("rosstat/all2013.csv", sep = ";")

def shorten(df, new_col_names, replace_names_dict):    
    df = df[new_col_names].rename(columns=replace_names_dict)
    data_cols = list(replace_names_dict.values())
    df[data_cols] = df[data_cols].applymap(lambda x: round(x / 10 ** 6, 1))    
    return df

from column_names import colname_to_varname_dict as SUB
MY_COLS = ['inn', 'year', 'okved1', 'region', 'title'] + \
          [x for x in SUB.keys()]

compact_df = shorten(df, MY_COLS, SUB)   
#check_balance(compact_df)

def check_balance(df):
    # активы = пассивы  
    flag1 = df.ta-df.tp
    assert abs(flag1).sum() < 15
    
    # внеоборотные активы + оборотные активы = активы
    flag2 = df.ta_fix + df.ta_nonfix - df.ta
    assert abs(flag2).sum() < 15
    
    # капитал + долгосрочные обязательства + краткосрочные обязательства = всего пассивы
    flag3 = df.tp_cap+df.tp_short+df.tp_long-df.tp
    assert abs(flag3).sum() < 15   
            
#save(compact_df, "projects_compact", folder='projects')