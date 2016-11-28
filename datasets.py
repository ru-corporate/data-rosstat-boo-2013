# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 12:50:03 2016

@author: Евгений
"""
#import csv
#import os
#from itertools import islice
#from collections import OrderedDict
#
#import pandas as pd
#
from reader import Dataset
from common import pipe
from copy import copy


def read_inns(path):
    gen = csv_stream(path)
    return list(r[0].replace("\ufeff", "") for r in gen 
                if not r[0].startswith("#"))

class Subset():

    ROOT = config.get_subset_root_folder()     
    
    def init_files(self, name):

        folder = os.path.join(self.ROOT, name)
        if not os.path.exists(folder):
            os.makedirs(folder)
            
        def in_dir(fn):    
            os.path.join(folder, fn)
             
        self.output_csv = in_dir(name+".csv")
        include_csv_path = in_dir("include.csv")
        exclude_csv_path = in_dir("exclude.csv")

        if os.path.exists(include_csv):
            self.includes = read_inns(include_csv_path)
        else:
            self.includes = []
            
        if os.path.exists(exclude_csv_path):
            self.excludes = read_inns(exclude_csv_path)
        else:
            self.excludes = []            
        
    
    def __init__(self, year, name):
        self.year=year         
        self.init_files(name)        
            
    def __get_stream__(self):
        return pipe(emit_rows_by_inn(self.year, inn_list=self.inn_list))



    #fn = config.make_path_for_inn_not_found_csv_file()
    #to_csv(fn, [[x] for x in inn_list])


#
#class Subset(Dataset):
#
#    @staticmethod
#    def __extract_inns__(gen):
#        return list(r[0].replace("\ufeff", "") for r in gen 
#                    if not r[0].startswith("#"))
#
#    def __init__(self, year):
#        self.year = year
#        self.output_csv = config.make_path_for_output_inn_csv_file(self.year)
#        inn_path = config.get_inn_list_path()
#        self.inn_list = self.__extract_inns__(csv_stream(inn_path))
#        self.msg = "\nSaving %s dataset with INN filter..." % self.year
#
#    def __get_stream__(self):
#        return pipe(emit_rows_by_inn(self.year, inn_list=self.inn_list))
