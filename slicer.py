# -*- coding: utf-8 -*-
""" Create reduced datasets based on boo_rosstat_*.csv files. """

from columns import RENAMER
from reader import Dataset
from config import make_path_for_user_output

FMT = dict(index=False, encoding="utf-8")

year = 2013
df = Dataset(year).read_df()

print("Changing units to mln rub")
## все исходные данные в тыс. рублей, преобразрованные - млн руб.
datacols = list(RENAMER.values())
df.loc[:,datacols]=(df.loc[:,datacols] / 1000).round(1)

#df[df.ta==df.ta.max()].title.iloc[0]
#Out[11]: 'Научно-Производственный Финансовый Концерн "ИНТЭКОТЕРРА"'
for x in 1001096, 1002168:
   df = df.drop(x)

df.plot.scatter(x='ta', y='sales').xlim(1000)
print("Firms with total assets above 30 mln rub or sales above 60 mln")
#total assets above 30 mln rub or sales above 5 mln rub per month
ix = (df.ta > 30) | (df.sales> 12*5)
df2 = df[ix]
fn = make_path_for_user_output(year, "main")
df2.to_csv(fn, **FMT)
print("Row count:", len(df2))
print("Saved:", fn)    
    
BLN = 10**3 # df already in rub million
bln = df[df.sales>BLN]    
fn = make_path_for_user_output(year, "bln")
bln.to_csv(fn, index=False)
print("Dataset: firms with sales > 1 bln rub")
print("Row count:", len(bln))    
print("Saved:", fn)
    
fn = make_path_for_user_output(year, 'xl_bln', ext=".xlsx")
bln.to_excel(fn, *FMT) 
print("Saved:", fn)   
                              
                            
#class TrimmedDataset():
#    
#    def __init__(self, year):
#        print("Year:", year)
#        self.df = Dataset(year).read_df()
#        
#    def save(self):           
#        slice_rows(self.df) 
                     
#TrimmedDataset(2012).save()

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