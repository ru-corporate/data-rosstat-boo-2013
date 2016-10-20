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
is_default = [x[1] for x in inn_iter()]    
