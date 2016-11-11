"""Download + unpack CSV file."""

import requests
import os
import subprocess

from config import UNPACK_RAR_EXE
from config import DATA_DIR

URL={2012:"http://www.gks.ru/opendata/storage/7708234640-bdboo2012/data-20161021t000000-structure-20121231t000000.rar"
   , 2013:"http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20161021t000000-structure-20131231t000000.rar"
   , 2014:"http://www.gks.ru/opendata/storage/7708234640-bdboo2014/data-20161021t000000-structure-20141231t000000.rar"
   , 2015:"http://www.gks.ru/opendata/storage/7708234640-bdboo2015/data-20161021t000000-structure-20151231t000000.rar"     
}


class RemoteDataset():   
        
    def __init__(self, year, silent=False):
        self.url = URL[year]
        self.silent = silent
        # RAR filename 
        rar_filename = self.url.split('/')[-1]          
        self.rar_path = os.path.join(DATA_DIR, rar_filename)
        # Rosstat CSV file       
        self.csv_path = os.path.join(DATA_DIR, self.rar_listing())

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
        
    def echo(self, msg, x):
        if not self.silent:
            print(msg, x)        
        
    def rar_listing(self):        
        return subprocess.check_output([
            UNPACK_RAR_EXE,
            'lb',self.rar_path]).decode("utf-8").strip()

    def download(self):        
        if os.path.exists(self.rar_path):
            self.echo("Already downloaded:", self.rar_path)
        else:
            self.echo("Downloading:", self.url)
            self._download(self.url, self.rar_path)    
            self.echo("Saved as:", self.rar_path)
        return self    

    def unrar(self):
        if os.path.exists(self.csv_path):
            self.echo("Already unpacked:", self.csv_path)
        else:         
            self._unrar(self.rar_path, DATA_DIR) 
            self.echo("Unpacked:", self.csv_path)
        return self.csv_path
        
if __name__=="__main__":
    fn = [RemoteDataset(x).download().rar_listing() for x in [2012,2013,2014,2015]]
    RemoteDataset(2015).unrar()