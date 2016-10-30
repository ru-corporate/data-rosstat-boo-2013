"""

    Download + unpack files original CSV file.
   
"""

import requests
import os
import platform
import subprocess

SOURCE_CSV_DIR = os.path.join("data")
SOURCE_CSV_PATH = os.path.join(SOURCE_CSV_DIR, "G2013.csv")
SOURCE_URL = "http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20150707t000000-structure-20131231t000000.rar"

IS_WINDOWS = (platform.system() == 'Windows')
if IS_WINDOWS:
    UNPACK_RAR_EXE = os.path.join('bin', 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'

class Downloader():
    
    def __init__(self, url, destination_folder=SOURCE_CSV_DIR):
        self.url = url
        filename = url.split('/')[-1]
        self.path = os.path.join(destination_folder, filename)     

    @staticmethod    
    def _download(url, path):
        r = requests.get(url.strip(), stream=True)
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        return path

    def download(self, overwrite=False):
        if not os.path.exists(self.path) or overwrite:
            print("Downloading:", self.url)
            self._download(self.url, self.path)    
            print("Saved as:", self.path)
        else:
            print("Already downloaded:", self.path)
        return self
    
    @staticmethod
    def _unrar(path, folder=SOURCE_CSV_DIR):        
        subprocess.check_call([
            UNPACK_RAR_EXE,
            'e', path,
            folder,
            '-y'
        ]) 

    def unrar(self, overwrite = False):
        if not os.path.exists(SOURCE_CSV_PATH) or overwrite:
            self._unrar(self.path) 
        else:
            print("Already unpacked:", SOURCE_CSV_PATH) 
            
if __name__ == "__main__":

    Downloader(SOURCE_URL).download().unrar()