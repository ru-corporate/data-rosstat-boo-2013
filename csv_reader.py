# -*- coding: utf-8 -*-
"""Read source CSV file and adjust numeric units."""

import csv
from remote import RemoteDataset
from row_parser import RowParser

def get_csv_lines(filename):
    with open(filename, 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';') # may use encoding="cp1251"
        for row in spamreader:
           if len(row)>1: #avoid reading last empty row 
                yield row           

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

    
class Reader():
    
    chunk = 10000
    TEST_CSV = "sample.txt"
    
    def __init__(self, year=2013):
        self.input_csv = RemoteDataset(year).download().unrar()
        self.output_csv = RemoteDataset(year).get_new_csv_filename()          
        self.parser = RowParser(year)
        self.columns = self.parser.columns        
        
    def adjust_row(self, row):
        return self.parser.adjust_row(row)  
        
    def parsed_rows(self, n=None, skip=0):
        if n:
            gen = csv_block(self.input_csv, n, skip)
        else:
            gen = get_csv_lines(self.input_csv)            
        i=1; k=0
        for r in gen:
            yield self.adjust_row(r)
            i+=1
            if i==self.chunk:
                i=0; k+=1
                print(self.chunk*k) 
                
    def save(self):
        gen = self.parsed_rows()
        to_csv(self.output_csv, gen, self.columns)
    
    def _demo(self):
        gen = self.parsed_rows(10, skip=50)        
        to_csv(self.TEST_CSV, gen, self.columns)
        
    def peep(self, skip=0):
        return next(self.parsed_rows(1,skip))
        
if __name__=="__main__":
    R = Reader(2013)
    a = next(R.parsed_rows(1))
    assert len(a)>200 
    R._demo()
    