# -*- coding: utf-8 -*-
"""Read Rosstat CSV file and adjust data and write to clean CSV files

boo_rosstat_2012.csv"

"""

import csv
import os
from remote import RemoteDataset
from row_parser import adjust_row, adjust_columns

from config import DATA_DIR

def get_csv_lines(filename):
    with open(filename, 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';') # may need to use encoding="cp1251"
        for row in spamreader:
           if len(row) == 266: #avoid reading last empty row and rows like ''Товарищество собственников жилья "Большевистская 111 "Б"',' 
               yield row
           else:
               print("Skipped row:", row)

def csv_block(filename, count, skip=0):
    k = 0 
    for i, row in enumerate(get_csv_lines(filename)):
        if i<skip:
            continue
        if k<count: 
            yield row
            k+=1
        else:
            break

def to_csv(path, gen, cols):    
    with open(path, 'w', encoding = "utf-8") as file:
        writer = csv.writer(file, delimiter=";", lineterminator="\n", 
                            quoting=csv.QUOTE_MINIMAL)
        writer.writerow(cols)
        writer.writerows(gen)
    print("Saved file:", path)    

    
class Dataset():
    
    chunk = 500000
    test_csv = "sample.txt"
    
    def __init__(self, year):
        self.year=year
        self.input_csv=RemoteDataset(year, silent=False).download().unrar()
        filename = "boo_rosstat_" + str(year) + ".csv"
        self.output_csv = os.path.join(DATA_DIR, filename)
        self.columns=adjust_columns()        
        
    def parsed_rows(self, n=None, skip=0):
        if n:
            gen = csv_block(self.input_csv, n, skip)
        else:
            gen = get_csv_lines(self.input_csv)            
        i=1; k=0
        for r in gen:
            yield adjust_row(r, self.year)
            i+=1
            if i==self.chunk:
                i=0; k+=1
                print(self.chunk*k) 
                
    def save(self):
        print("\nWriting", self.output_csv)
        gen = self.parsed_rows()
        to_csv(self.output_csv, gen, self.columns)
    
    def demo(self):
        for row in self.parsed_rows(3,skip=0):        
            print(" ".join(str(x) for x in row[0:4]))
        gen=self.parsed_rows(10,skip=0)
        to_csv(self.test_csv, gen, self.columns)
        
    def peek(self, skip=0):
        return next(self.parsed_rows(1,skip))
        
if __name__=="__main__":
    pass

#Uncomment below to create rosstat datasets
    #Dataset(2015).save()
    #Dataset(2014).save()
    #Dataset(2013).save()
    #Dataset(2012).save()
    
# peek into dataset 
    #dataset = Dataset(2015)
    #dataset.demo()
    #print(dataset.peek(361))