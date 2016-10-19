# -*- coding: utf-8 -*-
"""

Based on *all2013.csv* create following csv/xls files in *output* folder:

    bln.*
    large_with_debt.* (optional)
    projects.*

"""

import pandas as pd
import os

def save(df, filebase, folder="output"):
    pathbase = os.path.join(folder, filebase)    
    df.to_csv(pathbase + ".csv", sep = ";", index = False)
    df.to_excel(pathbase + ".xlsx", index = False)

df = pd.read_csv("output/all2013.csv", sep = ";")

BLN = 10**6
a = df[df['2110']>BLN]
save(a, "bln")

# file too large, not currently used
#b = df[(df['2110']>BLN) | (df['1410']>0)]
#save(b, "large_with_debt")

from names import inn_list
ix = df['inn'].isin(inn_list)
c = df[ix].sort_values(['okved1','2110'])
save(c, "projects")