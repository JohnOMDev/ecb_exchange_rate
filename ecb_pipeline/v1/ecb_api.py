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
import plotly.express as px
from plotly.io import to_html
from ecb_pipeline.src.ingestion import Extraction
from ecb_pipeline.src.transformation import Transformer
from datetime import datetime, timedelta
import logging
import os
import glob
import duckdb
import pandas as pd


logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger("ECB PIPELINE")
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
        extraction_handler.download_file(resp, partition_date)
        return "OK"
    except Exception as e:
        return f"Error: {str(e)}"

@v1.post("/ecb/history")
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
            min_date, max_date = data
            startPeriod = datetime.strftime(min_date, '%Y-%m-%d')
            endPeriod = datetime.strftime(max_date, '%Y-%m-%d')
            LOG.info(f"The min and max date for the historical order are: {startPeriod, endPeriod}")
            resp = extraction_handler.ecb_exchange_api(startPeriod, endPeriod)
            extraction_handler.download_file(resp, partition_date)
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

def read_prepared_sql() -> str:
    dir = join(settings.prepared_dir, "*", "*", "*.parquet")
    return f"read_parquet('{dir}', hive_partitioning = 1, hive_types_autocast = 0)"


@v1.get("/ecb/exchange_rate")
def get_exchange_rate_trends(
    currency_code: str = 'USD'
) -> list[dict]:
    """
    Plot the exhnage rate trends
    Parameters
    ----------

    Returns
    -------
    list[dict]

    """
    try:
        data = duckdb.sql(
            f"""
        INSTALL parquet;
        LOAD parquet;
        SET memory_limit = '5GB';
        SET threads TO 8;
        SELECT
            TIME_PERIOD,
            CURRENCY,
            exchange_rate
        FROM {read_prepared_sql()}
        WHERE TIME_PERIOD < '2023-05-01'
        ORDER BY TIME_PERIOD;
        """
        ).to_df()

        data['TIME_PERIOD'] = pd.to_datetime(data['TIME_PERIOD'])
        data = data[data['CURRENCY'] == currency_code]
        fig = px.line(
            data,
            x="TIME_PERIOD",
            y="exchange_rate",
            title=f"The exchange rate trends for {currency_code}",
        )

        plot_div = to_html(fig, full_html=False)

        html_content = f"""
        <html>
            <head>
                <title>Plot of {currency_code} over period of time</title>
            </head>
            <body>
                {plot_div}
            </body>
        </html>
        """
        return HTMLResponse(content=html_content)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Not not show the plot: {e}")



@v1.get("/ecb/currency")
def get_currency_analyses(
) -> list[dict]:
    """
    Analyse revenue generated in different currencies
    Parameters
    ----------

    Returns
    -------
    list[dict]

    """
    data = transform_handler.get_currency_analysis()
    if not data:
        return []
    return [
        {
            "currency_code": currency_code,
            "total_revenue": total_revenue,
        }
        for currency_code, total_revenue in data
    ]