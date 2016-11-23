"""Download + unpack CSV file."""

import requests
import os
import subprocess

from config import get_raw_csv_folder, get_rar_folder
from config import UNPACK_RAR_EXE, URL


RAW_CSV_FOLDER = get_raw_csv_folder()
RAR_FOLDER = get_rar_folder()

def in_rar_folder(filename):
    return os.path.join(RAR_FOLDER, filename)
    
def in_raw_csv_folder(filename):
    return os.path.join(RAW_CSV_FOLDER, filename)

class RawDataset():   

    # todo: why aren't they created?
    # config.make_dirs()       
        
    def __init__(self, year, silent=False):
        self.url = URL[year]
        self.silent = silent
        # RAR file path 
        rar_filename = self.url.split('/')[-1]          
        self.rar_path = in_rar_folder(rar_filename)    
        # Rosstat raw CSV file path 
        self.init_csv_filename()

    def init_csv_filename(self):
        if os.path.exists(self.rar_path):        
            csv_filename = self.rar_content()
            self.csv_path = in_raw_csv_folder(csv_filename)
        else:
            raise FileNotFoundError("RAR file not downloaded: " + self.rar_path)     

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

    # public methods download, unrar, get_filename          
    def download(self):        
        if not os.path.exists(self.rar_path):
            print("Downloading:", self.url)
            self._download(self.url, self.rar_path) 
            self.init_csv_filename()
            print("Saved as:", self.rar_path)
        else:
            print("Downloaded:", self.rar_path)        
        return self    

    def unrar(self):
        if not os.path.exists(self.csv_path):
            print("Unpacking:", self.csv_path)
            self._unrar(self.rar_path, folder=RAW_CSV_FOLDER) 
        else:
            print("Unpacked:", self.csv_path)
        return self.csv_path
        
    def get_filename(self):
        if os.path.exists(self.csv_path):
            return self.csv_path
        else:
            return self.download.unrar()
        
if __name__=="__main__":
    #RawDataset(2012).download().unrar()
    #RawDataset(2013).download().unrar()
    #RawDataset(2014).download().unrar()
    #RawDataset(2015).download().unrar()    
    print(RawDataset(2015).get_filename())