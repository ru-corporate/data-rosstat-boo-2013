# -*- coding: utf-8 -*-
"""Read list of INNs and industry tags from csv file like:

5040066582	AGRO
7707296041	TOURISM
6320002223	AUTOMOTIVE

"""

import csv

PATH = "inn.csv"

def inn_iterator():
    with open(PATH, 'r') as f:    
        reader = csv.reader(f, delimiter="\t")
        for line in reader:
            if line[0]:
               yield int(line[0]), line[1]

inn_dict = {k:v for k,v in inn_iterator()}              
inns = [x[0] for x in inn_iterator()]    


import pandas as pd
inn_df = pd.read_csv(PATH, delimiter = "\t", header = 0)