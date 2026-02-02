from argparse import ArgumentParser, Namespace
from bs4 import BeautifulSoup as Soup
from datetime import datetime
import logging
import json
import os
import pandas as pd
import polars as pl
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
import psycopg2
from shared.functions import log, log_execution, standardize_object_name
import re
import requests
from tqdm import tqdm
from typing import List
from urllib.parse import urljoin
import zipfile

DB_DSN = (
    "host={DB_HOST} "
    "port={DB_PORT} "
    "dbname={DB_DBNAME} "
    "user={DB_USER} "
    "password={DB_PASSWORD}"
).format(**os.environ)

TYPE_MAP = {
    "Int64": "BIGINT",
    "String": "VARCHAR",
    "Float64": "NUMERIC(18, 6)",
    "Boolean": "BOOLEAN"
}

REV_TYPE_MAP = {v:k for k,v in TYPE_MAP.items()}

JDBC_URL = "jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_DBNAME}".format(**os.environ)

def main():
    """
    * Pull raw data and load 
    """
    args = get_args(log)
    links = get_source_links(args, log)
    paths = download_unzip_data(links, args, log)
    load_raw_data(paths, args, log)

@log_execution
def get_args(log:logging.Logger) -> Namespace:
    """
    * Get command line arguments 
    """
    parser = ArgumentParser("pull_load_raw_data")
    parser.add_argument("--out_dir", required=True, help="Output directory for data.")
    parser.add_argument("--start_year", type=int, required=True, help="Start year for pulling data.")
    parser.add_argument("--end_year", type = int, required=True, help="End year for pulling data.")
    
    #args = parser.parse_args()

    args = Namespace(start_year=2019,
                     end_year=2024,
                     out_dir="/home/etl-user/raw_data")
    # Check the arguments:
    errs = []
    if args.start_year >= args.end_year:
        errs.append("start_year must be before end_year.")
    if errs:
        raise ValueError("\n".join(errs))

    return args

@log_execution
def get_source_links(args:Namespace, log:logging.Logger) -> List[str]:
    """
    * Retrieve all source links containing zip files.
    """
    url = "https://www.eia.gov/electricity/data/eia860/"
    # Find all .zip file urls:
    result = requests.get(url)
    # Build the search pattern for xls files:
    year_range = [str(y) for y in range(args.start_year, args.end_year + 1)]
    # Extract all of the urls containing .zip files for the years we want:
    soup = Soup(result.text, "lxml")
    zip_links = soup.select('a[href$=".zip"]')
    # Only return the links to the years we want:
    return [urljoin(url, t['href']) for t in zip_links if t["title"] in year_range]

@log_execution
def download_unzip_data(links:List[str], args:Namespace, log:logging.Logger) -> List[str]:
    """
    * Download the .zip files and unzip into the 
    specified output directory.
    """
    output_dirs = []
    log.info("Downloading data from %s links.", len(links))
    for link in tqdm(links):
        path = download_unzip_file(link, args, log)
        output_dirs.append(path)
    return output_dirs

@log_execution
def load_raw_data(output_dirs:List[str], args:Namespace, log:logging.Logger):
    """
    * Extract raw data from each .xlsx file and load
    into the raw destination table.
    """
    # Start the spark session with maven packages needed
    # to load into the postgres backend:
    spark = (SparkSession.builder
                         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8")
                         .getOrCreate())
    # Create the output 
    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)
    with open("config/table_map.json", "r") as f:
        table_map = json.load(f)
    log.info("Loading raw data into the backend.")
    with open("config/table_schemas.json", "r") as f:
        table_schemas = json.loads(f.read())
    with psycopg2.connect(dsn=DB_DSN).cursor() as cursor:
        for out_dir in output_dirs:
            log.debug("Loading %s.", out_dir)
            load_check_excel_files(spark, cursor, table_map, table_schemas, out_dir, log)

@log_execution
def load_check_excel_files(spark, cursor, table_map:dict, table_schemas:dict, out_dir:str, log:logging.Logger):
    """
    * Load excel files directly into target table
    """
    file_year = int(re.search(r"\d{4}$", out_dir)[0])
    for f in os.listdir(out_dir):
        if not f.endswith(".xlsx"):
            continue
        elif any(pt in f for pt in ["EIA-860 Form", "LayoutY"]):
            continue
        elif f.startswith("~"):
            continue
        parent = standardize_object_name(f)
        path = os.path.join(out_dir, f)
        file_ts = datetime.fromtimestamp(os.path.getctime(path))
        log.debug("Writing file %s.", path)
        xls = pd.ExcelFile(path)
        # Perform quality check:
        parent = standardize_object_name(f)
        #quality_check(xls, f, parent, table_map, log)
        for sheet in xls.sheet_names:
            df = pl.read_excel(path, read_options={"header_row": 1}, sheet_name=sheet)
            table_name = parent + "." + standardize_object_name(sheet)
            schema = table_schemas[table_name]
            # Add standard columns and transform existing ones to fit into
            # target types:
            cursor.execute(f"select * from {table_name} limit 1")
            col_order = [c.name for c in cursor.description]
            df = standardize_align_columns(df, col_order, schema, file_year, file_ts)
            spark_df = spark.createDataFrame(df.to_arrow(), schema=df.columns)
            upsert_to_db(spark_df, cursor, table_name, file_year, log)

@log_execution
def quality_check(xls:pd.ExcelFile, f:str, parent:str, table_map:dict, log:logging.Logger):
    """
    * Perform quality checks regarding expected schemas and others.
    """
    # Ensure that all sheets are present:
    missing = set(table_map[parent]) - set(xls.sheet_names)
    if missing:
        raise RuntimeError(f"The following sheets are missing from {f}: {','.join(missing)}")
    # Ensure that all columns are present:


@log_execution
def upsert_to_db(df:DataFrame, cursor, target_table:str, file_year:int, log:logging.Logger):
    #log.debug("Writing to %s.", DB_URL)
    # Upsert based on file year:
    cursor.execute(f"delete from {target_table} where file_year = {file_year}")
    props = {
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
    (df.write
       .mode("append")
       .jdbc(JDBC_URL, target_table, properties=props))

def standardize_align_columns(df:pl.DataFrame, col_order:List[str], schema:dict, file_year: int, file_ts:datetime):
    """
    * Add standard columns. 
    """
    # Transform the column names using the standard object naming method:
    df = df.rename({c: standardize_object_name(c)[:63] for c in df.columns})
    # Add standard columns:
    df = df.with_columns(
        pl.lit(file_year).cast(pl.Int32).alias("file_year"),
        pl.lit(file_ts).cast(pl.Datetime).alias("file_date"),
        pl.lit(datetime.now()).alias("upload_timestamp")
    )
    # Transform expected boolean columns:
    bool_cols = [c for c, tp in schema.items() if tp == "BOOLEAN"]
    for c, dtype in df.schema.items():
        if c not in bool_cols:
            continue
        elif dtype == pl.String:
            df = df.with_columns(
                pl.when(pl.col(c) == "Y").then(True)
                  .when(pl.col(c) == "N").then(False)
                  .otherwise(None)
                  .alias(c)
            )
    # Fill missing columns with NULLs:
    missing = [c for c in col_order if c not in df.columns]
    for m in missing:
        tp = REV_TYPE_MAP[schema[m]]
        tp = getattr(pl, tp)
        df = df.with_columns(pl.lit(None).cast(tp).alias(m))
    # Align the columns in order:
    return df.select(*col_order)

# Helpers:
def download_unzip_file(link:str, args:Namespace, log:logging.Logger):
    """
    * Download the file using streaming.
    """
    with requests.get(link, stream=True) as r:
        zip_path = re.search(r"(?P<f>\d+\.zip)$", link)["f"]
        zip_path = os.path.join(args.out_dir, zip_path)
        log.debug("Downloading file from %s to %s.", link, zip_path)
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        # Unzip the file:
        zip_out_dir = zip_path.replace(".zip", "")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(zip_out_dir)
        os.remove(zip_path)
        return zip_out_dir

if __name__ == "__main__":
    main()
