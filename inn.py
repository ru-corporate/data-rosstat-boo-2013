# -*- coding: utf-8 -*-
"""
Read INN list
"""

import csv
import os

PATH = os.path.join("projects", "inn.csv")

def inn_iter():
    with open(PATH, 'r') as f:    
        reader = csv.reader(f, delimiter="\t")
        for line in reader:
            if line[0]:
               yield int(line[0]), int(line[1])
 
inns = [x[0] for x in inn_iter()]    

from names import inn_list
inn_list = [int(x) for x in inn_list]

def fc(x,y):
     return [a for a in y if a not in x], [a for a in x if a not in y] 
     
z = [a for a in inn_list if a not in inns]