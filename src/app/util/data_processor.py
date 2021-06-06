import os
import sys
import pandas as pd

from glob import glob

util_dir = os.path.dirname(os.path.realpath(__file__))
app_dir = os.path.dirname(util_dir)
data_dir = os.path.join(app_dir, "data")
specs_dir = os.path.join(app_dir, "specs")

sys.path.append(util_dir)

import postgresql_uploader as PU

class DataProcessor:
    def __init__(self):
        self.specs = {}
        self.get_specs()

        self.data_files = {}
        self.db_conn = PU.PostgresqlConn()

        self.get_data_files()
        self.process_files()

    def check_files(self):
        self.data_files = {}
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
            processed_files = glob(os.path.join(data_dir, "*_processed.txt"))
            for i in self.data_files[spec]:
                print(f">>> {i}")
            for i in processed_files:
                print(f"--- {i}")

    def process_files(self):
        for spec, files in self.data_files.items():
            column_types = self.specs[spec]['column_types']

            for index, file in enumerate(files):
                df = self._get_text_to_df(file, self.specs[spec])

                new_name = file[:-4] + '_processed' + file[-4:]

                print(f'File old name: {file}')
                print(f'File new name: {new_name}')
                os.rename(file, new_name)

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

                    data[col] = row[start:start + length].strip()
                    start += length

                rows[index] = data

        return pd.DataFrame(rows)