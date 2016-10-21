# -*- coding: utf-8 -*-

import pandas as pd
import matplotlib.pyplot as plt
import os

df = pd.read_csv("projects_compact.csv", sep =";")

df = df.set_index('inn')

# ======================================================
#
#   Analysis of projects
#
# ======================================================


def hist(ts, file=None, bins=20):
    h = ts.hist(bins=bins)
    plt.show()
    if file:
        fig = h.get_figure()
        fig.savefig(os.path.join("png",file+".png"))
        plt.close()
        
df = df.drop([7707049388, 7709413138, 2130001337], axis = 0) 

# Фондоотдача
y = df.sales/df.of
y = y[y<1]
y = y[y>0.01]
hist(y, "sales_to_fixed_assets")

# Процентные ставки   
ir = df.cash_interest/(df.debt_long + df.debt_short) 
d = df.debt_long + df.debt_short 
ix = ir>0.01
ir = ir[ix] 
d = d[ix]
wmean = round(sum(ir * d) / sum(d) * 100, 1)
amean = round(ir.mean()*100, 1)
print(amean, wmean)
hist(ir, "interest_rates")

# dscr
ebitda_proxy = df.cash_oper_inflow - df.paid_to_supplier - df.paid_to_worker 
dscr = df.cash_interest / ebitda_proxy 
dscr = dscr[(abs(dscr) < 1000) & (dscr > 0.001)]
hist(dscr, bins=40, file="dscr")


# ФОТ и занятость 
fot = df.paid_to_worker.sum()
salary = 29792
ean=75528903
n = fot *10 ** 9 / (29792 * 12)
print (str(round(n/ean*100,2)) + "%") 

# оценка добавленной стоимости
va = df.cash_oper_inflow_sales - df.paid_to_supplier
y2 = va/df.of
ix = (y2>0.01) & (y2<2) 
hist(y2[ix], "va_to_assets")
print("Value added:", va.sum())
# 60.699999999999989

# основные средства
print(df.of.sum())




df.plot.scatter(x='exp_interest',y='cash_interest')



#todo:
#     need participation share   
#     taxes

# -----------------------------------------------------------------------------