from argparse import ArgumentParser, Namespace
from bs4 import BeautifulSoup as Soup
import logging
import os
import polars
from pyspark.sql import SparkSession
import re
import requests


def main():
    """
    
    """
    log = logging.getLogger()
    args = get_args(log)
    pull_load_raw_data(args, log)


def get_args(log:logging.Logger) -> Namespace:
    """
    * Get command line arguments 
    """
    parser = ArgumentParser("pull_load_raw_data")
    parser.add_argument("--out_dir", required=True, help="Output directory for data.")
    parser.add_argument("--start_year", required=True, help="Start year for pulling data.")
    parser.add_argument("--end_year", required=True, help="End year for pulling data.")
    
    args = parser.parse_args()
    # Check the arguments.
    errs = []
    if not re.match("\d{4}", args.start_year):
        errs.append(f"start_year {args.start_year} is invalid.")
    if not re.match("\d{4}", args.end_year):
        errs.append(f"end_year {args.end_year} is invalid.")
    if errs:
        raise RuntimeError("\n".join(errs))
    return args

def pull_load_raw_data(args:Namespace, log:logging.Logger):
    """
    * Pull the data and load into the backend.
    """
    spark = SparkSession.builder.getOrCreate()
    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)
    # Pull all of the excel spreadsheets from the website:
    url = "https://www.eia.gov/electricity/data/eia860/"
    # Find all .zip file urls:
    result = requests.get(url)
    soup = Soup(result.text, "lxml")
    soup.find_all("href")

    

if __name__ == "__main__":
    main()
