import os
import sys
import pandas as pd

from datetime import datetime
from glob import glob

util_dir = os.path.dirname(os.path.realpath(__file__))
app_dir = os.path.dirname(util_dir)
data_dir = os.path.join(app_dir, "data")
specs_dir = os.path.join(app_dir, "specs")
log_file = os.path.join(app_dir, "logs", f"log_{str(datetime.today().date())}.log")

sys.path.append(util_dir)
import postgresql_uploader as PU


class DataProcessor:
    def __init__(self):
        # Create the main dictionary for all of the metadata and df
        self.all_specs = {}
        self.data_files = {}

        # Initialize the postgres connection
        self.db_conn = PU.PostgresqlConn()

        # Start the process
        # 1. Get all the available specs in specs folder
        # 2. Get all data files based of the available specs
        # 3. Process the available files and store it in self.data_files
        self.get_specs()
        self.get_data_files()
        self.process_files()
        cleanup()

    def get_specs(self):
        """
        Gets all of the specs within the ./specs folder
        :return: stores all of the specs information within self.all_specs
        """
        specs_files = glob(os.path.join(specs_dir, "*"))

        for spec in specs_files:
            df = pd.read_csv(spec)

            spec_name = os.path.basename(spec).replace(".csv", "")
            info_list = df.to_dict("records")

            self.all_specs[spec_name] = {
                "detail_info": info_list,
                "column_names": [i['column name'] for i in info_list],
                "column_types": {i['column name']: i['datatype'] for i in info_list},
                "column_width": [i['width'] for i in info_list],
                "column_count": len(info_list)
            }

    def get_data_files(self):
        """
        Gets all of the data files based on the specs within the self.all_specs
        :return: stores all of the data files information in self.data_files {"spec_name": [file1.txt, file2.txt, ...]}
        """
        for spec in self.all_specs:
            self.data_files[spec] = glob(os.path.join(data_dir, f"{spec}*[!_processed].txt"))
            processed_files = glob(os.path.join(data_dir, "*_processed.txt"))

            # For clarity
            # Printing new files
            for i in self.data_files[spec]:
                print(f">>> {i}")
            for i in range(min(len(processed_files), 5)):
                print(f"--- {processed_files[i]}")
                if i == 4 and len(processed_files) - 5 > 0:
                    print(f"And {len(processed_files) - 5} other files")

    def process_files(self):
        """
        Process all of the files in self.data_files. Replace the list variables with pd.DataFrame
        :return: {"spec_name": [pd.DataFrame 1, pd.DataFrame 2, ...]}
        """
        for spec, files in self.data_files.items():
            column_types = self.all_specs[spec]['column_types']

            for index, file in enumerate(files):
                df = self._get_text_to_df(file, self.all_specs[spec])

                new_name = file[:-4] + '_processed' + file[-4:]
                os.rename(file, new_name)

                # Replace the file in files with df
                files[index] = df

            # Combine all of the files df into 1 called spec_df and apply the dtypes
            spec_df = pd.concat(files)

            # print(spec_df)

            # Get dtypes based off dtype dictionary in _data_types_converter
            file_dtype = self._data_types_converter(column_types)

            # We Convert the df dtypes using the
            spec_df = spec_df.astype(file_dtype)
            self.data_files[spec] = spec_df

    @staticmethod
    def _data_types_converter(column_types):
        """
        Convert the data types within the DataFrame to the correct format
        :param column_types: self.all_specs[spec_name]["column_types"]
        :return: returns a dictionary with the column name and type
        """
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
        """
        Read the text files and convert them to a df
        :param file: file location
        :param metadata: self.all_specs[spec_name]
        :return: pd.DataFrame
        """
        with open(file, 'r') as f:
            rows = f.readlines()

        new_rows = []

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
                    data_type = metadata["column_types"][col]

                    row_data = row[start:start + length].strip()

                    data[col] = int(row_data) if data_type == "INTEGER" or data_type == "BOOLEAN" else row_data
                    start += length

                new_rows.append(data)
            else:
                print(f"Row too long - {len(row)}/{row_length}: {row}")

        return pd.DataFrame(new_rows)


def cleanup():
    """
    Reset the file naming to the original
    :return: N/A
    """
    try:
        os.rename(
            os.path.join(data_dir, "testformat1_2020_06_28_processed.txt"),
            os.path.join(data_dir, "testformat1_2020_06_28.txt")
        )
    except FileNotFoundError as fe:
        print(f"{fe}")
