from argparse import ArgumentParser, Namespace
from bs4 import BeautifulSoup as Soup
import json
import logging
import os
import pandas as pd
import polars as pl
from shared.functions import download_unzip_file, log, log_execution, should_skip, standardize_object_name
import re
import requests
import textwrap
from tqdm import tqdm
from typing import List
from urllib.parse import urljoin

TYPE_MAP = {
    "Int64": "BIGINT",
    "String": "VARCHAR",
    "Float64": "NUMERIC(18, 6)",
    "Boolean": "BOOLEAN"
}

def main():
    """
    * 1. Extract all requested years' .zip files from the eia website.
    2. Use polars to determine appropriate postgres ddl that can store the columns,
    using the most conservative type allowable (preferring numeric unless mixed or contains ascii).
    3. Generate json files containing the following:
    - file_table_map.json: Maps the standardized { file_name -> sheet_name -> table_name }.
    - table_schemas.json: The final postgres column metadata for each table.
    - table_years.json: All years that the table has data for, based on file being present
    in the .zip file.
    - year_schemas.json: Each table column schema metadata by year. Used to determine the final ddl.
    4. Generate table_ddls.sql containing the raw postgres table ddls based on the
    column metadata.
    """
    args = get_args()
    links = get_source_links(args, log)
    paths = download_unzip_data(links, args, log)
    year_schemas, file_table_map = get_schema_info(paths, log)
    file_data = gen_ddls(year_schemas, args, log)
    file_data += [file_table_map]
    write_files(*file_data, log=log)

@log_execution
def get_args() -> Namespace:
    """
    * Get command line arguments 
    """
    description = "Generate ddls for each eia year's pulled data source."
    parser = ArgumentParser("gen_ddls", description=description)
    parser.add_argument("--start_year", type=int, required=True, help="Start year for parsing data.")
    parser.add_argument("--end_year", type=int, required=True, help="End year for parsing data.")
    parser.add_argument("--out_dir", default="/home/etl-user/raw_data", help="Output directory for data.")
    
    args = parser.parse_args()
    
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
        path = download_unzip_file(link, args.out_dir, log)
        output_dirs.append(path)
    return output_dirs

@log_execution
def get_schema_info(output_dirs:List[str], log:logging.Logger):
    """
    * Extract raw data from each .xlsx file and load
    into the raw destination table.
    """
    all_tps = set()
    year_schemas = {}
    table_map = {}
    for out_dir in output_dirs:
        log.debug("Loading %s.", out_dir)
        year = re.search(r"\d{4}$", out_dir)[0]
        year_schemas[int(year)], tps = parse_excel_files(out_dir, table_map, log)
        all_tps.update(tps)
    return year_schemas, table_map

@log_execution
def parse_excel_files(out_dir:str, table_map:dict, log:logging.Logger):
    """
    * Extract column metadata for each file's sheet and generate
    appropriate ddls.
    """
    all_tps = set()
    schemas = {}
    log.info("Parsing all files in directory %s.", out_dir)
    for f in os.listdir(out_dir):
        if should_skip(f):
            log.debug("Skipping %s.", f)
            continue
        path = os.path.join(out_dir, f)
        parent = standardize_object_name(f)
        # Get the sheet names (need to use pandas because polars cannot read Excel metadata):
        xls = pd.ExcelFile(path)
        sheets = xls.sheet_names
        report = {}
        table_map[parent] = table_map.get(parent, {})
        for sheet in sheets:
            log.debug("Sheet %s.", sheet)
            table_name = parent + "." + standardize_object_name(sheet)
            table_map[parent][sheet] = table_name
            df = pl.read_excel(path, read_options={"header_row": 1}, sheet_name=sheet)
            # Include the distinct count to identify
            # unique columns:
            exprs = []
            for c, dtype in df.schema.items():
                log.debug("Column %s:%s.", c, dtype)
                exprs.append(pl.col(c).n_unique().alias(f"{c}_distinct"))
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
                    "max_len": sheet_report[f"{col}_max_len"],
                    "col_type": str(tp)
                }
                for col, tp in df.schema.items()
            }
        all_tps.update(df.schema.values())
        # Sort in 
        schemas[parent] = dict(sorted(report.items(), key=lambda x: x[0]))
    return dict(sorted(schemas.items(), key=lambda x: x[0])), table_map

@log_execution
def gen_ddls(year_schemas:dict, args:Namespace, log:logging.Logger):
    """
    * Generate the most conservative ddls possible for each table
    across all years based on the column metadata.
    """
    # Group all data by table across years.
    log.info("Generating all ddls.")
    table_years = {}
    grouped_schemas = {}
    years = list(range(args.start_year, args.end_year+1))
    for year, table_schemas in year_schemas.items():
        for _, sheet_schema in table_schemas.items():
            for table_name, schema in sheet_schema.items():
                grouped_schemas[table_name] = grouped_schemas.get(table_name, [])
                grouped_schemas[table_name].append(schema)
                table_years[table_name] = table_years.get(table_name, set())
                table_years[table_name].add(year)
    table_years = {nm: list(yrs) for nm, yrs in table_years.items()}
    # Check if any tables missing for any years:
    if any(len(schemas) != len(years) for schemas in grouped_schemas.values()):
        missing = [tbl for tbl,schemas in grouped_schemas.items() if len(schemas) != len(years)]
        print(f"The following are not present for all years: {','.join(missing)}")
        missing_years = {nm: list(set(years) - set(table_years[nm])) for nm in missing}
        print(json.dumps(missing_years, indent=2))
    # Generate ddls based on column attributes across years.
    schema_names = set([tbl.split(".")[0] for tbl in grouped_schemas])
    ddls = sorted([f"CREATE SCHEMA {sch};" for sch in schema_names])
    ddls.append("\n")
    table_schemas = {}
    for tbl, schemas in grouped_schemas.items():
        ddl, schema = generate_table_ddl(tbl, schemas)
        table_schemas[tbl] = schema
        ddls.append(ddl + "\n")
    return [ddls, year_schemas, table_schemas, table_years]

# Helpers:
def generate_table_ddl(table_name:str, table_schemas:List[dict]) -> str:
    """
    * Generate a table ddl based upon all
    metadata.
    """
    log.info("Generating ddl for table %s.", table_name)
    col_meta = {}
    for schema in table_schemas:
        for c, meta in schema.items():
            log.debug("Column %s", c)
            # Note that we need to use the first 63 characters only
            # because postgres truncates them:
            col_name = standardize_object_name(c)[:63]
            col_meta[col_name] = col_meta.get(col_name, {})
            col_meta[col_name]["col_type"] = col_meta[col_name].get("col_type", set())
            col_meta[col_name]["is_distinct"] = col_meta[col_name].get("is_distinct", set())
            col_meta[col_name]["max_len"] = col_meta[col_name].get("max_len", set())
            col_meta[col_name]["col_type"].add(get_column_type(meta))
            col_meta[col_name]["is_distinct"].add(meta["is_distinct"])
            col_meta[col_name]["max_len"].add(meta["max_len"])
    col_ddls = []
    table_schema = {}
    for nm in col_meta:
        is_distinct = all(col_meta[nm]["is_distinct"])
        # Get the column type:
        if len(col_meta[nm]["col_type"]) == 1:
            tp = list(col_meta[nm]["col_type"])[0]
        else:
            tp = "String"
        mapped_tp = TYPE_MAP[tp]
        table_schema[nm] = mapped_tp
        max_len = None
        if max_len:
            mapped_tp += f"({max_len})"
        col_ddl = f"{nm} {mapped_tp}"
        if is_distinct and tp == "Int64":
            col_ddl += " PRIMARY KEY"
        col_ddls.append(col_ddl)
    # Include file date, file year and upload timestamp columns:
    col_ddls.append("file_year INT NOT NULL")
    col_ddls.append("file_date TIMESTAMP NOT NULL")
    col_ddls.append("upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL")
    table_ddl = [f"CREATE TABLE {table_name}("]
    table_ddl.append(textwrap.indent(",\n".join(col_ddls), "\t"))
    table_ddl.append(");")
    table_ddl = "\n".join(table_ddl)
    return table_ddl, table_schema

def get_column_type(meta:dict) -> str:
    """
    * Get the column type based on the metadata.
    """
    if meta["distinct_count"] == 2:
        return "Boolean"
    return meta["col_type"]

def write_files(ddls:List[str], 
                year_schemas:dict, 
                table_schemas:dict, 
                table_years:dict, 
                file_table_map:dict,
                log:logging.Logger):
    """
    * Output the final json metadata files.
    """
    log.info("Writing all generated files to ./config.")
    with open("config/table_ddls.sql", "w") as f:
        f.write("\n".join(ddls))
    with open("config/year_schemas.json", "w") as f:
        f.write(json.dumps(year_schemas, indent=2))
    with open("config/table_schemas.json", "w") as f:
        f.write(json.dumps(table_schemas, indent=2))
    with open("config/file_table_map.json", "w") as f:
        f.write(json.dumps(file_table_map, indent=2))
    with open("config/table_years.json", "w") as f:
        f.write(json.dumps(table_years, indent=2))

if __name__ == "__main__":
    main()
