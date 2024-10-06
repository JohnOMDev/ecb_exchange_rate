#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 04 18:40:21 2024

@author: johnomole
"""
from ecb_pipeline.settings import Settings, DBCredentials
import os
import duckdb
import psycopg
import logging
import gzip
import json
import shutil
from contextlib import contextmanager
from opentelemetry import trace
tracer = trace.get_tracer(__name__)
logger = logging.getLogger(__name__)

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))

settings = Settings()


class Transformer:
    def __init__(self):
        self.db_credentials = DBCredentials()
        self.default_db = f"dbname={self.db_credentials.dbname} user={self.db_credentials.user} host={self.db_credentials.host} password={self.db_credentials.password} port=5432"

    @contextmanager
    def duckdbconnect(self):
        con = None
        try:
            con = duckdb.connect()
            yield con
        except Exception as e:
            LOG.info(f"problem with the duckdb connection: {e}")
        finally:
            if con:
                con.close()

    @contextmanager
    def psqlconnect(self):
        try:
            con = psycopg.connect(self.default_db)
            yield con
            con.commit()
        except Exception as e:
            LOG.info(f"problem with the postgres connection: {e}")
            con.rollback()
        finally:
            con.close()

    @tracer.start_as_current_span("export_ecb_data_to_psql")
    def export_ecb_data_to_psql(self, _file_dir: list[str]) -> None:
        """

        Parameters
        ----------
        _file_dir : list

        Returns
        -------
        None

        """
        
        LOG.info(f"Processing file: {_file_dir}")
        query = f"""
                     INSTALL parquet;
                     LOAD parquet;
                     SET memory_limit = '25GB';
                     SET threads TO 8;
                     SELECT TIME_PERIOD, CURRENCY, exchange_rate
                     FROM read_parquet({_file_dir});
                     """
        try:
            with self.duckdbconnect() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
            #    return rows
                LOG.info("Data successfully fetched from parquet.")

            if not rows:
                LOG.warning("No data fetched from DuckDB.")
                return

            data = [
                (row[0], row[1], row[2]) for row in rows
            ]

            self.insert_ecb_currency_exchange(data)

        except Exception as e:
            LOG.error(f"An error occurred: {e}")

    @tracer.start_as_current_span("insert_ecb_currency_exchange")
    def insert_ecb_currency_exchange(self, ecb_data: list[tuple]) -> None:
        query = """
            INSERT INTO exchange_rates (date,currency_code,rate_to_eur)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, currency_code)
            DO UPDATE SET
                rate_to_eur = EXCLUDED.rate_to_eur;
        """
        try:
            with self.psqlconnect() as con:
                with con.cursor() as cur:
                    if ecb_data:
                        cur.executemany(query, ecb_data)
                        LOG.info("successfully Ended")
        except Exception as e:
            LOG.error(f"Failed to execute query: {e}")

    @tracer.start_as_current_span("create_new_column_order_table")
    def create_new_column_order_table(self)  -> None:
        query = """
            ALTER TABLE orders ADD COLUMN order_revenue_eur NUMERIC;
        """
        
        try:
            if not self.check_order_table_for_order_revenue_eur():
                with self.psqlconnect() as con:
                    with con.cursor() as cur:
                        cur.execute(query)
                        LOG.info("successfully Ended")
        except Exception as e:
            LOG.error(f"Failed to execute query: {e}")

    @tracer.start_as_current_span("check_order_table_for_order_revenue_eur")
    def check_order_table_for_order_revenue_eur(self)  -> list[tuple]:
        query = """
            SELECT * 
            FROM information_schema.columns
            WHERE table_name = 'orders' AND column_name = 'order_revenue_eur';
        """
        
        try:
            with self.psqlconnect() as con:
                with con.cursor() as cur:
                    cur.execute(query)
                    data = cur.fetchone()
                    return data
                    LOG.info("successfully Ended")
        except Exception as e:
            LOG.error(f"Failed to execute query: {e}")
            

    @tracer.start_as_current_span("apply_currency_conversionn")
    def apply_currency_conversionn(self)  -> None:
        query = """
            UPDATE orders o 
            SET order_revenue_eur = 
                CASE WHEN o.currency_code='EUR' THEN o.revenue 
                     ELSE o.revenue / COALESCE(er.rate_to_eur, 1) END
            FROM exchange_rates er
            WHERE o.order_date=er."date"
            AND er.currency_code=o.currency_code;
        """
        
        try:
            with self.psqlconnect() as con:
                with con.cursor() as cur:
                    cur.execute(query)
                    LOG.info("successfully Ended")
        except Exception as e:
            LOG.error(f"Failed to execute query: {e}")

    @tracer.start_as_current_span("get_historical_order_exchange_rate")
    def get_historical_order_exchange_rate(self)  -> list[tuple]:
        query = """
            SELECT Min(order_date), Max(order_date)
            FROM orders r
            LEFT JOIN exchange_rates er ON r.order_date=er."date" AND er.currency_code=r.currency_code
            WHERE order_revenue_eur is NULL;
        """
        
        try:
            with self.psqlconnect() as con:
                with con.cursor() as cur:
                    cur.execute(query)
                    data = cur.fetchone()
                    return data
                    LOG.info("successfully Ended")
        except Exception as e:
            LOG.error(f"Failed to execute query: {e}")
            

    @tracer.start_as_current_span("to_parquet")
    def to_parquet(self, raw_dir, prepared_file) -> None:
        duckdb.sql("INSTALL parquet;")
        duckdb.sql("LOAD parquet;")
        logging.info("Reading from tmp dir")

        if os.path.exists(prepared_file):
            shutil.rmtree(prepared_file)

        query = f""" INSTALL parquet; 
                    LOAD parquet; 
                    SET memory_limit = '5GB';
                    SET threads TO 4;
                    COPY (SELECT 
                              TIME_PERIOD as date,
                              TIME_PERIOD,
                              OBS_VALUE as exchange_rate,
                              currency,
                              FREQ as frequency
                          FROM read_csv('{raw_dir}/*') ORDER BY TIME_PERIOD, CURRENCY
                          ) TO '{prepared_file}'
                    (FORMAT PARQUET, partition_by (date));
                    """
        try:
            with self.duckdbconnect() as cursor:
                cursor.execute(query)
            LOG.info("successfully Ended")
        except Exception as e:
            LOG.error(f"Failed to execute query: {e}")

