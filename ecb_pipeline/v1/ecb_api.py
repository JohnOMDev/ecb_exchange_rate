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
import multiprocessing
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


@v1.post("/ecb/download")
def download_bremen_state_data() -> str:
    """
    Downloads and extracts Bremen state data.
    Returns
    -------
    str
        suuccess or error.

    """
    download_dir = os.path.join(settings.raw_dir, "day=20241001")
    os.makedirs(download_dir, exist_ok=True)

    try:
        startPeriod = "2024-09-01"
        endPeriod = "2024-09-30"
        resp = extraction_handler.ecb_exchange_api(startPeriod, endPeriod)
        extraction_handler.download_file(resp)
    except Exception as e:
        return f"Error: {str(e)}"
    return "OK"


@v1.post("/ecb/prepare")
def prepare_data() -> str:
    """
    Prepare and transform Bremen state data. Export them into geoparquet
    Returns
    -------
    str
        suuccess or error.

    """
    raw_dir = os.path.join(settings.raw_dir, "day=20241001")
    prepared_file = os.path.join(settings.prepared_dir, "day=20241001")

    os.makedirs(prepared_file, exist_ok=True)

    try:
        transform_handler.to_parquet(raw_dir, prepared_file)
    except Exception as e:
        return f"Error: {str(e)}"
    return "OK"


@v1.post("/ecb/analytics")
def prepare_analytics() -> str:
    """
    Create an analytical table and perform upsert of the data into the table: buildings and parcels.
    Returns
    -------
    str
        suuccess or error.

    """
    prepared_file = os.path.join(settings.prepared_dir, "day=20241001")
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
    except Exception as e:
        return f"Error: {str(e)}"
    return "OK"
