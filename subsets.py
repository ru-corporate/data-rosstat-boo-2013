# -*- coding: utf-8 -*-

from slicer import df, save 
    
SUBSETS_FOLDER = "rosstat/subsets"

BLN = 10**6
a = df[df['2110']>BLN]
save(a, "bln", folder=SUBSETS_FOLDER)
    
b = df[(df['2110']>BLN) | (df['1410']>0)]
save(b, "large_with_debt", folder=SUBSETS_FOLDER)
