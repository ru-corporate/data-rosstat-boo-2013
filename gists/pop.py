# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 17:38:53 2016

@author: Евгений
"""

z = {'11003': 3164657,
 '11104': 150,
 '11203': 0}
 
for k, v in z.items():
    if k.endswith("3") and not k.startswith("3"):
        new_key = k[0:-1]
        z[new_key] = z.pop(k)
        
for k, v in z.items():
    if k.endswith("4"):
        new_key = k[0:-1]
        z[new_key] = z.pop(k)        