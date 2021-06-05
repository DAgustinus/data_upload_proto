import os
import sys
import logging

import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from glob import glob

cur_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(cur_dir, "data")
specs_dir = os.path.join(cur_dir, "specs")

class DataProcessor:
    def __init__(self):
        self.specs = {}
        self.data_files = {}

        self.get_specs()
        self.get_data_files()

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
            for index, file in enumerate(files):
                records = self._get_text_to_records(file, self.specs[spec])
                df = pd.DataFrame(records)

                # Replace the file in files with df
                files[index] = df

            # Get dtypes based off dtype dictionary in _data_types_converter
            file_dtype = self._data_types_converter(self.specs[spec]['column_types'])

            # Combine all of the files df into 1 called spec_df and apply the dtypes
            spec_df = pd.concat(files).astype(file_dtype)

            self.data_files[spec] = spec_df


    def _data_types_converter(self, column_types):
        dictionary = {
            "TEXT": str,
            "INTEGER": int,
            "FLOAT": float,
            "BOOLEAN": bool,
            "DATETIME": 'datetime64[ns]'
        }

        out_types = {}

        for c_name, c_type in column_types.items():
            try:
                out_types[c_name] = dictionary[c_type]
            except ValueError as VE:
                print(VE)

        return out_types

    def _get_text_to_records(self, file, metadata):
        with open(file, 'r') as f:
            rows = f.readlines()

        print(rows)

        for index, row in enumerate(rows):
            # Check if the row_length is less than or equal to the max length
            row_length = sum(metadata["column_width"])
            row = row.strip()
            data = {}

            if row <= row_length:
                start = 0
                for i in range(metadata["column_count"]):
                    length = metadata["column_width"][i]
                    col = metadata["column_names"][i]

                    data[col] = row[start:start+length]
                    start += length

                rows[index] = data

        return rows

def main():
    pass

if __name__ == "__main__":
    main()