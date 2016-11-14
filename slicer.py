# -*- coding: utf-8 -*-
""" Create reduced datasets based on boo_rosstat_*.csv files. """

import pandas as pd
import os


from columns import RENAMER
from reader import Dataset
from config import CSV_DIR

fmt = dict(index=False, encoding="utf-8")

NEW_COLS = ['year', 'title', 'inn', 'ok1', 'ok2', 'ok3', 'region'] + list(RENAMER.keys())
DATA_COLS = list(RENAMER.values())


def generate_output_filename(year, prefix, ext=".csv"):
    return os.path.join(DATA_DIR, prefix + "_" + str(year) + ext)

    
def subset_columns(df):
    df = df[NEW_COLS].rename(columns=RENAMER)
    fn = generate_output_filename(year, "boo")
    df.to_csv(fn, **fmt)
    print("Saved:", fn)                                                       
                                                           
def slice_rows(df):
    
    #total assets above 30 mln rub or sales above 5 mln rub per month
    ix = (df.ta > 30000) | (df.sales> 12*5000)
    df = df[ix]
    
    ## все исходные данные в тыс. рублей, преобразрованные - млн руб.
    df.loc[:,DATA_COLS]=(df.loc[:,DATA_COLS] / 1000).round(1)
        
    fn = generate_output_filename(year, "main")
    df.to_csv(fn, **fmt)
    print("Dataset: firms with total assets above 30 mln rub or sales above 60 mln")
    print("Row count:", df.count())
    print("Saved:", fn)    
    
    BLN = 10**3 # df already in rub million
    bln = df[df.sales>BLN]    
    fn = generate_output_filename(year, 'bln')
    bln.to_csv(fn, index=False)
    print("Dataset: firms with sales > 1 bln rub")
    print("Row count:", bln.count())    
    print("Saved:", fn)
    
    fn = generate_output_filename(year, 'bln', ext=".xlsx")
    bln.to_excel(fn, *fmt) 
    print("Saved:", fn)   
                              
                            
class TrimmedDataset():
    
    def __init__(self, year):
        print("Year:", year)
        self.df = Dataset(year).read_df()
        
    def save(self):           
        slice_rows(self.df) 

                      
TrimmedDataset(2012).save()

                      
# Uncomment below to create rosstat datasets
# Dataset(2015).save()
# Dataset(2014).save()
# Dataset(2013).save()
# Dataset(2012).save()                        
               


               
# for year in [2013, 2014, 2015]:
    # df = read_rosstat_csv(year)
    
    # df = df[NEW_COLS].rename(columns=RENAMER)
    # fn = name(year, "boo")
    # df.to_csv(fn, **fmt)
    # print("Saved:", fn)
    
    # ix = (df.ta > 30000) | (df.sales> 12*5000)
    # subset1 = df[ix]
    # все исходные данные в тыс. рублей, преобразрованные - млн руб.
    # subset1.loc[:,DATA_COLS]=(subset1.loc[:,DATA_COLS] / 1000).round(1)
    # fn = name(year, "main")
    # subset1.to_csv(fn, **fmt)
    # print("Saved:", fn)
    
    # BLN = 10**3
    # bln = subset1[subset1.sales>BLN]    
    # fn = name(year, 'bln')
    # bln.to_csv(fn, index=False)
    # print("Saved:", fn)
    
    # fn = name(year, 'bln', ext=".xlsx")
    # bln.to_excel(fn, *fmt) 
    # print("Saved:", fn)                       
                         


#df.to_csv("data/G2013_merged.csv", index=False, encoding="utf-8")
#
#
#def flags(df):
#    # активы = пассивы
#    flag1 = df.ta-df.tp
#    # внеоборотные активы + оборотные активы = активы    
#    flag2 = df.ta_fix + df.ta_nonfix - df.ta
#    # капитал + долгосрочные обязательства + краткосрочные обязательства = всего пассивы
#    flag3 = df.tp_cap+df.tp_short+df.tp_long-df.tp
#    return flag1, flag2, flag3
#
## проверка АвтоВАЗ
#vaz = df[df.inn == 6320002223]
#assert (vaz.sales == 175152000).all()
#
## уменьшаем размер выборки
#ix = (df.ta > 10000) | (df.sales> 12*5000)
#subset1 = df[ix]
## все исходные данные в тыс. рублей, преобразрованные - млн руб.
#data_cols = list(rename_dict.values())
#subset1[data_cols]=(subset1[data_cols] / 1000).round(1)
#subset1.to_csv("data/main.csv", **file_param) 
#
## предприятия с выручкой свыше 1 млрд. руб. 
#BLN = 10**3
#bln = subset1[subset1.sales>BLN]
#bln.to_csv("data/bln.csv", **file_param)
#bln.to_csv("data/bln.xlsx", **file_param)  