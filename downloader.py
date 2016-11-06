"""

    Download + unpack files original CSV file.
   
"""

import requests
import os
import subprocess

from config import SOURCE_URL, DATA_DIR, SOURCE_CSV_PATH 
from config import UNPACK_RAR_EXE


class Downloader():
    
    def __init__(self, url=SOURCE_URL, destination_folder=DATA_DIR):
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
    def _unrar(path, folder):        
        subprocess.check_call([
            UNPACK_RAR_EXE,
            'e', path,
            folder,
            '-y'
        ]) 

    def unrar(self, overwrite = False):
        if not os.path.exists(SOURCE_CSV_PATH) or overwrite:
            self._unrar(self.path, DATA_DIR) 
        else:
            print("Already unpacked:", SOURCE_CSV_PATH) 
        return SOURCE_CSV_PATH
            
if __name__ == "__main__":
    Downloader().download().unrar()