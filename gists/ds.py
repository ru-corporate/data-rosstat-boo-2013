# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 18:19:40 2016

@author: Евгений
"""

import dataset 

db = dataset.connect('sqlite:///:memory:')
table = db['boo']

table.insert(dict(name='John Doe', age=37))
table.insert(dict(name='Jane Doe', age=34, gender='female'))

john = table.find_one(name='John Doe')