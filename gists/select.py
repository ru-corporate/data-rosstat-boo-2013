# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 20:10:53 2016

@author: Евгений
"""

import dataset 
db = dataset.connect('sqlite:///boo.slite3')


import time


result = db.query("""SELECT COUNT(*) as cnt FROM boo""")
for x in result:
   print(x['cnt'])
#time.sleep(15)