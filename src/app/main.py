import os
import sys
import logging
import random
import time

import numpy as np
import pandas as pd

from util import data_processor as dp
from util import file_creator as fc


def main():
    print("App Started")
    d = dp.DataProcessor()

    time.sleep(10)
    print("--- Sleeping 10")

    while True:
        time.sleep(5)
        print("--- Sleeping 5")
        # print(d.data_files)

        if random.randint(0, 1) == 1:
            print("Creating another file")
            fc.FileCreator(row_length=random.randint(5, 15))
            d.check_files()

        for spec, df in d.data_files.items():
            if not isinstance(df, pd.DataFrame):
                continue
            print("Sending data to Postgres")
            print(f"-- Sending data to {spec} table")

            d.db_conn.df_to_psql(df, spec)

            print(f"-- Sending completed")
            print(f"-- Checking data back | {spec}")

            query = f"select count(*) from {spec}"
            result = d.db_conn.query_psql(query)
            print(result)

            d.data_files[spec] = None

if __name__ == "__main__":
    main()
