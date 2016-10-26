# -*- coding: utf-8 -*-
"""Create *projects.xlsx* based on *all2013.csv* and *inn.csv*"""

import pandas as pd
from collections import OrderedDict
import os

def save(df, filebase, folder=None):
    if folder:
        pathbase = os.path.join(folder, filebase)    
    else:
        pathbase = filebase
    df.to_csv(pathbase + ".csv", sep = ";", index = False, encoding = "utf-8")
    df.to_excel(pathbase + ".xlsx", index = False)


#df = pd.read_csv("rosstat/all2013.csv", sep = ";")
inn_df = pd.read_csv('inn.csv', delimiter = "\t", 
                     header = 0, names = ['inn','tag'])

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
save(project_df, "projects_compact", folder='projects')

#
#   Create files with fewer columns
#

rename =  OrderedDict([
 ('year', 'year'), 
 ('okved1', 'okved1'), 
 ('region', 'region'), 
 ('title', 'title'),
 ('inn', 'inn'), 
 ('1110', '1110'),
 ('1120', '1120'),
 ('1130', '1130'),
 ('1140', '1140'),
 ('1150', 'of'),   # основные средства
 ('1160', '1160'),
 ('1170', '1170'),
 ('1180', '1180'),
 ('1190', '1190'),
 ('1100', 'ta_fix'), #внеоборотные активы

 ('1210', '1210'),
 ('1220', '1220'),
 ('1230', '1230'),
 ('1240', '1240'),
 ('1250', '1250'),
 ('1260', '1260'),
 ('1200', 'ta_nonfix'), #оборотные активы

 ('1600', 'ta'), # активы всего

 ('1310', '1310'),
 ('1320', '1320'),
 ('1340', '1340'),
 ('1350', '1350'),
 ('1360', '1360'),
 ('1370', '1370'),
 ('1300', 'tp_cap'), # капитал

 ('1410', 'debt_long'), # долгосрочные займы
 ('1420', '1420'),
 ('1430', '1430'),
 ('1450', '1450'),
 ('1400', 'tp_long'), #долгосрочные обязательства 

 ('1510', 'debt_short'), # кракосрочные займы
 ('1520', '1520'),
 ('1530', '1530'),
 ('1540', '1540'),
 ('1550', '1550'),
 ('1500', 'tp_short'), #краткосрочные обязательства 
 
 ('1700', 'tp'),    # пассивы всего 
 
 ('2110', 'sales'), # выручка
 ('2120', '2120'),
 ('2100', '2100'),
 ('2210', '2210'),
 ('2220', '2220'),
 ('2200', '2200'),
 ('2310', '2310'),
 ('2320', '2320'),
 ('2330', 'exp_interest'), # процентные платежи 
 ('2340', '2340'),
 ('2350', '2350'),
 ('2300', '2300'),

 ('2410', '2410'),
 ('2421', '2421'),
 ('2430', '2430'),
 ('2450', '2450'),
 ('2460', '2460'),
 ('2400', '2400'),

 ('2510', '2510'),
 ('2520', '2520'),
 ('2500', '2500'),

 ('4110', 'cash_oper_inflow'),        # поступления вcего 
 ('4111', 'cash_oper_inflow_sales'),  # поступления от продаж
 ('4112', '4112'),
 ('4113', '4113'),
 ('4119', '4119'),
 ('4120', '4120'),
 ('4121', 'paid_to_supplier'),  # платежи поставщикам
 ('4122', 'paid_to_worker'),    # платежи работникам
 ('4123', 'cash_interest'),     # процентные платежи  
 ('4124', '4124'),
 ('4129', '4129'),
 ('4100', '4100'),

 ('4210', '4210'),
 ('4211', '4211'),
 ('4212', '4212'),
 ('4213', '4213'),
 ('4214', '4214'),
 ('4219', '4219'),
 ('4220', '4220'),
 ('4221', '4221'),
 ('4222', '4222'),
 ('4223', '4223'),
 ('4224', '4224'),
 ('4229', '4229'),
 ('4200', '4200'),

 ('4310', '4310'),
 ('4311', '4311'),
 ('4312', '4312'),
 ('4313', '4313'),
 ('4314', '4314'),
 ('4319', '4319'),
 ('4320', '4320'),
 ('4321', '4321'),
 ('4322', '4322'),
 ('4323', '4323'),
 ('4329', '4329'),
 ('4300', '4300'),

 ('4400', '4400'),

 ('4490', '4490')]) # изменение, связанное с курсовой переоценкой
           

sub = OrderedDict([(k,v) for k,v in rename.items() if k!=v])      
my_cols = ['year', 'okved1', 'region', 
           'title',  'inn', 'tag'] + [x for x in sub.keys()]
data_cols =  [x for x in sub.values()]

aggr_dict = {"Фрештел (Конс)":[7727560086, 7718571010, 
                              7710646874, 7701641245]}
# do aggregation


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
