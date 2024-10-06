#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 04 15:15:30 2024

@author: johnomole
"""

from fastapi import APIRouter, status, HTTPException
from fastapi.responses import HTMLResponse
from concurrent.futures import ThreadPoolExecutor
from ecb_pipeline.settings import Settings
from os.path import join
from ecb_pipeline.src.ingestion import Extraction
from ecb_pipeline.src.transformation import Transformer
from datetime import datetime, timedelta
import logging
import os
import glob
import duckdb


logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger("bremen state")
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))


conn = duckdb.connect()
settings = Settings()

v1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            "description": "Something is wrong with the request"
        },
    },
    prefix="/api/v1",
    tags=["v1"],
)
extraction_handler = Extraction()
transform_handler = Transformer()

data_date = datetime.strftime(datetime.now()-timedelta(days=1), '%Y-%m-%d')
partition_date = datetime.strftime(datetime.now()-timedelta(days=1), '%Y%m%d')

@v1.post("/ecb/download")
def download_daily_exchange_rate() -> str:
    """
    Downloads and extracts the daily exchange rate from the european central bank API.
    Returns
    -------
    str
        suuccess or error.

    """
    download_dir = os.path.join(settings.raw_dir, f"day={partition_date}")
    os.makedirs(download_dir, exist_ok=True)

    try:
        startPeriod = data_date
        endPeriod = data_date
        resp = extraction_handler.ecb_exchange_api(startPeriod, endPeriod)
        extraction_handler.download_file(resp)
        return "OK"
    except Exception as e:
        return f"Error: {str(e)}"

@v1.post("/ecb/analytics")
def get_historical_exchange_rate() -> str:
    """
    Downloads and extracts the historical exchange rate from the european central bank API.
    Returns
    -------
    str
        suuccess or error.

    """
    download_dir = os.path.join(settings.raw_dir, f"day={partition_date}")
    os.makedirs(download_dir, exist_ok=True)

    try:
        data = transform_handler.get_historical_order_exchange_rate()
        if data:
            startPeriod, endPeriod = data
            resp = extraction_handler.ecb_exchange_api(startPeriod, endPeriod)
            extraction_handler.download_file(resp)
            return "OK"
        else:
            return "No historical data to retrived!"
    except Exception as e:
        return f"Error: {str(e)}"

@v1.post("/ecb/prepare")
def prepare_data() -> str:
    """
    Prepare and transform the excahnge rate data. Export them into parquet file storage
    Returns
    -------
    str
        suuccess or error.

    """
    raw_dir = os.path.join(settings.raw_dir, f"day={partition_date}")
    prepared_file = os.path.join(settings.prepared_dir, f"day={partition_date}")

    os.makedirs(prepared_file, exist_ok=True)

    try:
        transform_handler.to_parquet(raw_dir, prepared_file)
        return "OK"
    except Exception as e:
        return f"Error: {str(e)}"
    


@v1.post("/ecb/analytics")
def prepare_analytics() -> str:
    """
    Create an analytical table and perform upsert of the data into the table: exchange_rate,
    Create new column to convert the revenue amount to eur.
    Apply the revenue amount of each order to eur.
    
    Returns
    -------
    str
        suuccess or error.

    """
    prepared_file = os.path.join(settings.prepared_dir, f"day={partition_date}")
    prepared_filenames = glob.glob(f"{prepared_file}/*/*.parquet")
    batch_size = 4

    
    batches = [
        (prepared_filenames[i : i + batch_size])
        for i in range(0, len(prepared_filenames), batch_size)
    ]

    try:
        for batch in batches:
            transform_handler.export_ecb_data_to_psql(batch)
            
        transform_handler.create_new_column_order_table()

        transform_handler.apply_currency_conversionn()

        logging.info("export ended")
        return "OK"
    except Exception as e:
        return f"Error: {str(e)}"

