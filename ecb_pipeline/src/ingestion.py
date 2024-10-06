#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 04 18:40:21 2024

@author: johnomole
"""
from ecb_pipeline.settings import Settings
import requests
import os
import logging


logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))

settings = Settings()


class Extraction:
    def __init__(self):
        self.url = 'https://data-api.ecb.europa.eu/service/data/EXR/D.PLN+USD+JPY+GBP.EUR.SP00.A'

    def ecb_exchange_api(self, startPeriod: str = '2024-01-01', endPeriod: str = '2024-09-20'):
        params = {
            'startPeriod': startPeriod,
            'endPeriod': endPeriod
        }
        response = requests.get(self.url, params=params, headers={'Accept': 'text/csv'})
        if response.status_code == 200:
            return response



    def download_file(self, response) -> None:
        download_dir = os.path.join(settings.raw_dir, "day=20241001")
        os.makedirs(download_dir, exist_ok=True)
        filename = f'{download_dir}/ecb_exchange.csv'
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=128 * 1024):
                f.write(chunk)
                
                
                
                
                
                
                
                
            