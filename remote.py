"""Download + unpack CSV file."""

import requests
import os
import subprocess

from config import UNPACK_RAR_EXE

from config import DATA_DIR
RAR_DIR = DATA_DIR
CSV_DIR = DATA_DIR

LOCATIONS = {
   2013 : dict(url="http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20150707t000000-structure-20131231t000000.rar"
        , csv_file="G2013.csv")
}

class RemoteDataset():   
        
    def __init__(self, year):
        self.url = LOCATIONS[year]['url']
        # RAR filename 
        rar_filename = self.url.split('/')[-1]          
        self.rar_path = os.path.join(RAR_DIR, rar_filename)
        # Original CSV filename        
        csv_filename = LOCATIONS[year]['csv_file']
        self.source_csv_path = os.path.join(CSV_DIR, csv_filename)
        # New CSV filename
        base, ext = csv_filename.split(".")
        csv_filename_2 = base + "_ready." + ext
        self.modified_csv_path = os.path.join(CSV_DIR, csv_filename_2)

    @staticmethod    
    def _download(url, path):
        r = requests.get(url.strip(), stream=True)
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        return path

    @staticmethod
    def _unrar(path, folder):        
        subprocess.check_call([
            UNPACK_RAR_EXE,
            'e', path,
            folder,
            '-y'
        ]) 

    def download(self, overwrite=False):
        if os.path.exists(self.rar_path) or not overwrite:
            print("Already downloaded:", self.rar_path)
        else:
            print("Downloading:", self.url)
            self._download(self.url, self.rar_path)    
            print("Saved as:", self.rar_path)        
        return self    

    def unrar(self, overwrite = False):
        if os.path.exists(self.source_csv_path) or not overwrite:
            print("Already unpacked:", self.source_csv_path)
        else:         
            self._unrar(self.rar_path, RAR_DIR) 
        return self.source_csv_path
        
    def get_new_csv_filename(self):
        return self.modified_csv_path
            
if __name__ == "__main__":   
    assert RemoteDataset(2013).download().unrar() == 'data\\G2013.csv'
    assert RemoteDataset(2013).get_new_csv_filename() == 'data\\G2013_ready.csv'