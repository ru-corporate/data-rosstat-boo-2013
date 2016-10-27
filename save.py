import requests
import os

SOURCE_CSV_DIR = os.path.join("data", "rosstat")
SOURCE_URL = "http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20150707t000000-structure-20131231t000000.rar"

class URL_Downloader():
    
    def __init__(self, url=SOURCE_URL, folder=SOURCE_CSV_DIR):
        self.url = url
        filename = url.split('/')[-1]
        self.path = os.path.join(folder, filename)     

    @staticmethod    
    def download(url, path):
        r = requests.get(url.strip(), stream=True)
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        return path

    def get(self, overwrite=False):
        if not os.path.exists(self.path) or overwrite:
            print("Downloading:", self.url)
            self.download(self.url, self.path)    
            print("Saved as:", self.path)
        else:
            print("Already downloaded:", self.path)

if __name__=="__main__":
    URL_Downloader().get()