"""Download + unpack CSV file."""

import requests
import os
import subprocess

from config import UNPACK_RAR_EXE
from config import from_rar_folder, from_raw_csv_folder, raw_csv_folder
from config import URL, VALID_YEARS

class RemoteDataset():   
        
    def __init__(self, year, silent=False):
        self.url = URL[year]
        self.silent = silent
        # RAR file path 
        rar_filename = self.url.split('/')[-1]          
        self.rar_path = from_rar_folder(rar_filename)    
        # Rosstat raw CSV file path      
        csv_filename = self.rar_content()
        self.csv_path = from_raw_csv_folder(csv_filename)        

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
        
    def rar_content(self):
        """List single filename stored in RAR archive."""    
        return subprocess.check_output([
            UNPACK_RAR_EXE,
            'lb',self.rar_path]).decode("utf-8").strip()    
        
    def echo(self, msg, x):
        MSG_OFFSET = '%19s'
        if not self.silent:
            print(MSG_OFFSET % msg , x)

    # public methods download and unrar          
    def download(self):        
        if not os.path.exists(self.rar_path):
            self.echo("Downloading:", self.url)
            self._download(self.url, self.rar_path)    
            self.echo("Saved as:", self.rar_path)
        else:
            self.echo("Already downloaded:", self.rar_path)        
        return self    

    def unrar(self):
        if not os.path.exists(self.csv_path):
            self._unrar(self.rar_path, folder=raw_csv_folder()) 
            self.echo("Unpacked:", self.csv_path)
            return None
        else:
            self.echo("Already unpacked:", self.csv_path)
            return self.csv_path
        
if __name__=="__main__":
    RemoteDataset(2012).download().unrar()
    RemoteDataset(2013).download().unrar()
    RemoteDataset(2014).download().unrar()
    RemoteDataset(2015).download().unrar()    