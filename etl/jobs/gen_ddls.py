from argparse import ArgumentParser, Namespace
from bs4 import BeautifulSoup as Soup
import json
import logging
import os
import pandas as pd
import polars as pl
from shared.functions import log, log_execution, standardize_object_name
import re
import requests
import textwrap
from tqdm import tqdm
from typing import List
from urllib.parse import urljoin
import zipfile

TYPE_MAP = {
    "Int64": "BIGINT",
    "String": "VARCHAR",
    "Float64": "DECIMAL(32, 32)",
    "Boolean": "BOOLEAN"
}


def main():
    """
    * 
    """
    args = get_args(log)
    links = get_source_links(args, log)
    paths = download_unzip_data(links, args, log)
    schemas, table_map = get_schema_info(paths, log)
    with open("config/schemas.json", "w") as f:
        f.write(json.dumps(schemas, indent=2))
    gen_ddls(schemas, table_map, args, log)

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

def get_schema_info(output_dirs:List[str], log:logging.Logger):
    """
    * Extract raw data from each .xlsx file and load
    into the raw destination table.
    """
    all_tps = set()
    schemas = {}
    table_map = {}
    for out_dir in output_dirs:
        log.debug("Loading %s.", out_dir)
        year = re.search(r"\d{4}$", out_dir)[0]
        schemas[int(year)], tps = parse_excel_files(out_dir, table_map, log)
        all_tps.update(tps)
    return schemas, table_map

@log_execution
def parse_excel_files(out_dir:str, table_map:dict, log:logging.Logger):
    """
    * Load excel files directly into target table
    """
    all_tps = set()
    schemas = {}
    for f in os.listdir(out_dir):
        if not f.endswith(".xlsx"):
            continue
        elif any(pt in f for pt in ["EIA-860 Form", "LayoutY"]):
            continue
        elif f.startswith("~"):
            continue
        path = os.path.join(out_dir, f)
        parent = standardize_object_name(f)
        log.debug("Writing file %s.", path)
        # Get the sheets:
        xls = pd.ExcelFile(path)
        sheets = xls.sheet_names
        report = {}
        table_map[parent] = table_map.get(parent, {})
        for sheet in sheets:
            table_name = parent + "." + standardize_object_name(sheet)
            table_map[parent][sheet] = table_name
            df = pl.read_excel(path, read_options={"header_row": 1}, sheet_name=sheet)
            # Include the distinct count to identify
            # unique columns:
            exprs = []
            for c, dtype in df.schema.items():
                exprs.append(pl.col(c).n_unique().alias(f"{c}_distinct"))
                exprs.append(pl.col(c).null_count().gt(0).alias(f"{c}_has_nulls"))
                exprs.append(
                    (
                        pl.col(c).str.len_chars().max()
                        if dtype == pl.String
                        else pl.lit(None, dtype=pl.Int64)
                    ).alias(f"{c}_max_len")
                )
            sheet_report = df.select(exprs).to_dicts()[0]
            n_rows = df.count().shape[1]
            report[table_name] = {
                col.replace("_distinct", ""): {
                    "distinct_count": sheet_report[f"{col}_distinct"],
                    "is_distinct": sheet_report[f"{col}_distinct"] == n_rows,
                    "has_nulls": sheet_report[f"{col}_has_nulls"],
                    "max_len": sheet_report[f"{col}_max_len"],
                    "col_type": str(tp)
                }
                for col, tp in df.schema.items()
            }
        all_tps.update(df.schema.values())
        schemas[parent] = dict(sorted(report.items(), key=lambda x: x[0]))
    return dict(sorted(schemas.items(), key=lambda x: x[0])), table_map

def gen_ddls(schema_info:dict, table_map:dict, args:Namespace, log:logging.Logger):
    """
    * Generate the most conservative ddls possible for each table
    across all years based on the column metadata.
    """
    # Group all data by table across years.
    grouped_schemas = {}
    table_years = {}
    years = list(range(args.start_year, args.end_year+1))
    for year, table_schemas in schema_info.items():
        for parent, sheet_schema in table_schemas.items():
            for table_name, schema in sheet_schema.items():
                grouped_schemas[table_name] = grouped_schemas.get(table_name, [])
                grouped_schemas[table_name].append(schema)
                table_years[table_name] = table_years.get(table_name, set())
                table_years[table_name].add(year)
    table_years = {nm: list(yrs) for nm, yrs in table_years.items()}
    with open("config/table_map.json", "w") as f:
        f.write(json.dumps(table_map, indent=2))
    with open("config/table_years.json", "w") as f:
        f.write(json.dumps(table_years, indent=2))
    # Check if any tables missing for any years:
    if any(len(schemas) != len(years) for schemas in grouped_schemas.values()):
        missing = [tbl for tbl,schemas in grouped_schemas.items() if len(schemas) != len(years)]
        print(f"The following are not present for all years: {','.join(missing)}")
        missing_years = {nm: list(set(years) - set(table_years[nm])) for nm in missing}
        print(json.dumps(missing_years, indent=2))
    # Use the earliest schemas as the quality check:
    
    # Generate ddls based on column attributes across schemas:
    # Extract all schemas to generate:
    schema_names = set([tbl.split(".")[0] for tbl in grouped_schemas])
    ddls = sorted([f"CREATE SCHEMA {sch};" for sch in schema_names])
    ddls.append("\n")
    for tbl, schemas in grouped_schemas.items():
        ddl = generate_table_ddl(tbl, schemas)
        ddls.append(ddl + "\n")
    with open("ddls.sql", "w") as f:
        f.write("\n".join(ddls))

# Helpers:
def generate_table_ddl(table_name:str, table_schemas:List[dict]) -> str:
    """
    * Generate a table ddl based upon all
    metadata.
    """
    # Group by column attributes:
    col_meta = {}
    for schema in table_schemas:
        for c, meta in schema.items():
            col_name = standardize_object_name(c)
            col_meta[col_name] = col_meta.get(col_name, {})
            col_meta[col_name]["col_type"] = col_meta[col_name].get("col_type", set())
            col_meta[col_name]["is_distinct"] = col_meta[col_name].get("is_distinct", set())
            col_meta[col_name]["not_null"] = col_meta[col_name].get("not_null", set())
            col_meta[col_name]["max_len"] = col_meta[col_name].get("max_len", set())
            col_meta[col_name]["col_type"].add(get_column_type(meta))
            col_meta[col_name]["is_distinct"].add(meta["is_distinct"])
            col_meta[col_name]["not_null"].add(not meta["has_nulls"])
            col_meta[col_name]["max_len"].add(meta["max_len"])
    col_ddls = []
    for nm in col_meta:
        is_distinct = all(col_meta[nm]["is_distinct"])
        not_null = all(col_meta[nm]["not_null"])
        # Get the column type:
        if len(col_meta[nm]["col_type"]) == 1:
            tp = list(col_meta[nm]["col_type"])[0]
        else:
            tp = "String"
        mapped_tp = TYPE_MAP[tp]
        #max_len = max(col_meta[col_name]["max_len"]) if col_meta[col_name]["max_len"] else None
        max_len = None
        if max_len:
            mapped_tp += f"({max_len})"
        col_ddl = f"{nm} {mapped_tp}"
        if is_distinct and tp == "Int64":
            col_ddl += " PRIMARY KEY"
        elif not_null:
            col_ddl += " NOT NULL"
        col_ddls.append(col_ddl)
    # Include file date, file year and upload timestamp columns:
    col_ddls.append("file_year INT NOT NULL")
    col_ddls.append("file_date TIMESTAMP NOT NULL")
    col_ddls.append("upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL")
    table_ddl = [f"CREATE TABLE {table_name}("]
    table_ddl.append(textwrap.indent(",\n".join(col_ddls), "\t"))
    table_ddl.append(");")
    table_ddl = "\n".join(table_ddl)
    return table_ddl

def get_column_type(meta:dict) -> str:
    """
    * Get the column type based on the metadata.
    """
    if meta["distinct_count"] == 2:
        return "Boolean"
    return meta["col_type"]

def max_string_len(df: pl.DataFrame, col: str):
    if df.schema[col] == pl.Utf8:
        return df.select(pl.col(col).str.len_chars().max()).item()
    return None
    
def get_meta(meta):
    if not meta["has_nulls"]:
        return "NOT NULL"

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
