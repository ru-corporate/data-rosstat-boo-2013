# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 17:02:23 2016

@author: Евгений
"""

import g13

a = next(g13.get_csv_lines())
assert isinstance(a['name'], str)  