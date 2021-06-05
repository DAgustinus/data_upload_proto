import os
import sys
import logging
import time

import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from glob import glob
from util import postgresql_uploader as PU

cur_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(cur_dir, "data")
specs_dir = os.path.join(cur_dir, "specs")

class DataProcessor:
    def __init__(self):
        self.specs = {}
        self.data_files = {}
        self.db_conn = PU.PostgresqlConn()

        self.get_specs()
        self.get_data_files()

        self.process_files()

    def get_specs(self):
        specs_files = glob(os.path.join(specs_dir, "*"))

        for spec in specs_files:
            df = pd.read_csv(spec)

            spec_name = os.path.basename(spec).replace(".csv", "")
            info_list = df.to_dict("records")

            self.specs[spec_name] = {
                "detail_info": info_list,
                "column_names": [i['column name'] for i in info_list],
                "column_types": {i['column name']: i['datatype'] for i in info_list},
                "column_width": [i['width'] for i in info_list],
                "column_count": len(info_list)
            }

    def get_data_files(self):
        for spec in self.specs:
            self.data_files[spec] = glob(os.path.join(data_dir, "*[!_processed].txt"))

    def process_files(self):
        for spec, files in self.data_files.items():
            column_types = self.specs[spec]['column_types']

            for index, file in enumerate(files):
                df = self._get_text_to_df(file, self.specs[spec])

                # Replace the file in files with df
                files[index] = df

            # Get dtypes based off dtype dictionary in _data_types_converter
            file_dtype = self._data_types_converter(column_types)

            # Combine all of the files df into 1 called spec_df and apply the dtypes
            spec_df = pd.concat(files)

            if 'BOOLEAN' in column_types.values():
                for key, col_type in column_types.items():
                    if col_type == 'BOOLEAN':
                        try:
                            spec_df[key] = spec_df[key].astype(int)
                        except ValueError as VE:
                            print(f"Column {key} is not originally an Int based column")

            spec_df = spec_df.astype(file_dtype)
            self.data_files[spec] = spec_df

    @staticmethod
    def _data_types_converter(column_types):
        out_types = {}
        dictionary = {
            "TEXT": str,
            "INTEGER": int,
            "FLOAT": float,
            "BOOLEAN": bool,
            "DATETIME": 'datetime64[ns]'
        }

        for c_name, c_type in column_types.items():
            try:
                out_types[c_name] = dictionary[c_type]
            except ValueError as VE:
                raise VE

        return out_types

    @staticmethod
    def _get_text_to_df(file, metadata):
        with open(file, 'r') as f:
            rows = f.readlines()

        for index, row in enumerate(rows):
            # Check if the row_length is less than or equal to the max length
            row_length = sum(metadata["column_width"])
            row = row.strip()
            data = {}

            if len(row) <= row_length:
                start = 0
                for i in range(metadata["column_count"]):
                    length = metadata["column_width"][i]
                    col = metadata["column_names"][i]

                    data[col] = row[start:start+length].strip()
                    start += length

                rows[index] = data

        return pd.DataFrame(rows)


def main():
    print("App Started")
    d = DataProcessor()

    time.sleep(20)
    print("--- Sleeping 20")

    while True:
        time.sleep(5)
        print("--- Sleeping 5")
        # print(d.data_files)

        print("Sending data to Postgres")
        for spec, df in d.data_files.items():
            print(df)
            print(f"-- Sending data to {spec} table")
            d.db_conn.df_to_psql(df, spec)
            print(f"-- Sending completed")
            print(f"-- Checking data back | {spec}")

            query = f"select * from {spec}"
            result = d.db_conn.query_psql(query)
            print(result)


if __name__ == "__main__":
    main()
