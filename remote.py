# -*- coding: utf-8 -*-
"""Download and unpack CSV file from Rosstat web site."""

import requests
import os
import subprocess

from folders import Folder
from config import UNPACK_RAR_EXE, URL

class RawDataset():

    def __init__(self, year):
        self.year = year
        self.url = URL[year]
        # RAR file path
        rar_filename = self.url.split('/')[-1]
        self.rar_path = Folder('rar').filepath(rar_filename)
        # Rosstat raw CSV file path
        self._init_csv_filename()

    def _init_csv_filename(self):
        if os.path.exists(self.rar_path):
            csv_filename = self._rar_content()
            self.csv_path = Folder('raw_csv').filepath(csv_filename) 
        else:
            self.csv_path = ''

    @staticmethod
    def _download(url, path):
        r = requests.get(url.strip(), stream=True)
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
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

    def _rar_content(self):
        """Return single filename stored in RAR archive."""
        return subprocess.check_output([
            UNPACK_RAR_EXE,
            'lb', self.rar_path]).decode("utf-8").strip()

    # public methods download, unrar, get_filename
    def download(self):
        if os.path.exists(self.rar_path):
            print("Already downloaded:", self.rar_path)
        else:
            print("Downloading:", self.url)
            self._download(self.url, self.rar_path)
            print("Saved as:", self.rar_path)
            self._init_csv_filename()
        return self

    def unrar(self):
        if os.path.exists(self.csv_path):
            print("Already unpacked:", self.csv_path)
        else:
            print("Unpacking:", self.csv_path)
            self._unrar(self.rar_path, folder=Folder('raw_csv').path())
        return self.csv_path

    # todo: change name to 'get_raw_csv_path' using IDE
    def get_filename(self):
        if os.path.exists(self.csv_path):
            return self.csv_path
        else:
            msg = "Source CSV file for year {} not found. ".format(self.year) +\
                  "\nUse RawDataset({}).download().unrar() to proceed.".format(self.year)
            raise FileNotFoundError(msg)


if __name__ == "__main__":
    # RawDataset(2012).download().unrar()
    # RawDataset(2013).download().unrar()
    # RawDataset(2014).download().unrar()
    # RawDataset(2015).download().unrar()
    print(RawDataset(2012).get_filename())
