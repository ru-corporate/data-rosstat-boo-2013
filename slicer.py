# -*- coding: utf-8 -*-
"""Create *projects.xlsx* based on *all2013.csv* and *inn.csv*"""

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


INN_CSV_COLUMNS = ['inn','tag']
inn_df = pd.read_csv('inn.csv', delimiter = "\t", 
                     header = 0, names = INN_CSV_COLUMNS)
inn_dups = inn_df[inn_df.duplicated()]
print("INN configuration file has %d duplicated rows" % inn_dups.count().inn)
print(inn_dups.inn)
inn_df = inn_df.drop_duplicates()


project_df = pd.merge(inn_df, df, on='inn', how='left')
project_df['is_found'] = project_df.year.notnull()
not_found_count = sum([not x for x in project_df['is_found']])
print("%d INN(s) not found in database" % not_found_count)
print(project_df[~project_df['is_found']].inn)

project_df = project_df.sort_values(['is_found','okved1','2110'],
                                    ascending=[False, True, True])

assert not project_df.duplicated().all() # no duplicate rows 
# -------------------------------------
aggr_dict = {"Фрештел (Конс)":[7727560086, 7718571010, 
                              7710646874, 7701641245]}
# do aggregation
# -------------------------------------
save(project_df, "projects_compact", folder='projects')





from column_names import colname_to_varname_dict as sub
my_cols = INN_CSV_COLUMNS  + \
          ['year', 'okved1', 'region', 'title'] + \
          [x for x in sub.keys()]
data_cols =  [x for x in sub.values()]



compact_df = project_df[my_cols].rename(columns=sub)
for col in data_cols:
    compact_df[col] = (compact_df[col] / 10 ** 6).round(1) 

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
 
# 
# WARNING:
#     df may have duplicate rows
 
            
save(compact_df, "projects_compact", folder='projects')
