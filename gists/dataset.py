# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 01:41:45 2016

@author: Евгений
"""

#import dataset 

#db = dataset.connect('sqlite:///:memory:')
#db = dataset.connect('sqlite:///boo.slite3')
#table = db['boo']

#table.insert_many(gen)    
    
#result = db.query('PRAGMA table_info(boo)')
#for row in result:
#    print(row)

#result = db.query("""SELECT title, 
#                            round(_1410/1000000,1) as debt, 
#                            round(_2110/1000000,1) as sales 
#                            FROM boo \
#                  WHERE (year = 2013) AND \
#                        (sales > 0.1 or debt > 0.1)""")
#for row in result:
#   print(row['title'], row['sales'],  row['debt'])    


#gen = db.query("""SELECT * FROM boo WHERE (year = 2013) \
#                  AND (_2110 > 1000000 or _1410 > 1000000)""")