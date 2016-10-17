# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 02:07:11 2016

@author: Евгений
"""

ROUNDING_ERROR = 2

FILENAME = "_bln2013.csv" #"_all2013.csv"
import pandas as pd
z = pd.read_csv(FILENAME, sep = ";")

ROUNDING_ERROR = 2

#todo: do create table and insert by sqla

# баланс
ap  = [x for x in z.columns if x.startswith("_1")]

# отчет о прибыли и убытках
# todo - проверки сумм
opu = [x for x in z.columns if x.startswith("_2")] 

# отчет о движении денежных средств
# todo - проверки сумм
cf  = [x for x in z.columns if x.startswith("_4")] 

# Todo: show companies where asserts 
#       does not equal rounding error 

# _1600 - активы всего
# _1700 - пассивы всего 
flag1 = abs(z['_1700']-z['_1600'])<ROUNDING_ERROR

# _1100 - внеоборотные активы
# _1200 - оборотные активы
flag2 = abs(z['_1100']+z['_1200']-z['_1600'])<ROUNDING_ERROR

# _1300 - капитал
# _1400 - долгосрочные обязательства
# _1500 - краткосрочные обязательства
# _1700 - всего пассивы
flag3 = abs(z['_1300']+z['_1400']+z['_1500']-z['_1700'])<ROUNDING_ERROR

#d = z[['inn','okved','title','name','_2110','_1410']]
#s = d[d['_1410']>0].sort_values('okved')
#s.to_csv("with_debt.csv",sep=";", index=False)
#s.to_excel("with_debt.xlsx")
