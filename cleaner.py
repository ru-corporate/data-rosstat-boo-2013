# -*- coding: utf-8 -*-
"""
Purge 'broken' rows from raw csv:
 - wrong number of columns
 - company has no INN field

Saves clean csv as new file. 
"""

import config
from remote import RemoteDataset
from csv_access import csv_stream, to_csv, indicate_progress_by_chunk
from columns import VALID_SOURCE_CSV_ROW_WIDTH

INN_POSITION_IN_ROW = 5

def extract_inn(row):
    """Access INN field in row."""
    return row[INN_POSITION_IN_ROW]

class Logger():
    """Log errors into file while reading rows."""
    def __init__(self, log_filename):
        self.log_filename=log_filename 
        with open(self.log_filename,'w') as f:
            print("Log started", file=f)
            
    def report(self, *msg):        
        with open(self.log_filename,'a') as f:
            print(*msg)
            print(*msg, file=f)
             
class Cleaner():
    """Make readable version of raw csv file."""

    def __init__(self, year):
        print("Cleaning raw CSV file for year:", year) 
        self.source_csv = RemoteDataset(year).unrar()
        self.clean_csv = config.make_path_clean_csv(year)
        log_path = config.make_path_error_log(year)
        self.logger = Logger(log_path)
        
    def get_source_rows(self):
        gen = csv_stream(self.source_csv)
        for row in indicate_progress_by_chunk(gen):
            yield row
            
    def get_filtered_rows(self):
    
        def echo_line_error(i,row,reason):
            self.logger.report("\nError in row", i+1)
            self.logger.report(reason)                 
            self.logger.report("Row:", row)           
    
        for i, row in enumerate(self.get_source_rows()):
            if len(row) != VALID_SOURCE_CSV_ROW_WIDTH:
                reason = "Invalid row length {}".format(str(len(row)))
                echo_line_error(i,row,reason)
            elif not extract_inn(row):
                reason = "Skipped row with empty INN field"
                echo_line_error(i,row,reason)          
            else:
                yield row
            
    def run(self):
        if not os.path.exists(self.clean_csv):
            gen = self.get_filtered_rows() 
            to_csv(path=self.clean_csv, stream=gen)   
        else:
            self.echo("Already cleaned:", self.clean_csv )        
        return self.clean_csv
            
if __name__ == "__main__":
    Cleaner(2012).run()            
    Cleaner(2013).run()   
    Cleaner(2014).run()    
    Cleaner(2015).run()        
    

