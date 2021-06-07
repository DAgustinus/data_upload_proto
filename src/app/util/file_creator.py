import os
import sys
import faker
import string
import random
import pandas as pd

from datetime import datetime
from glob import glob

util_dir = os.path.dirname(os.path.realpath(__file__))
app_dir = os.path.dirname(util_dir)
data_dir = os.path.join(app_dir, "data")


class FileCreator:
    def __init__(self, row_length=10):
        # initialize variables
        self.row_length = row_length
        self.specs = {}

        # run functions to start
        self._get_spec()
        self.create_file()

    def create_file(self):
        for spec, metadata in self.specs.items():
            # print(f"SPEC: {spec}")
            rows = []
            for i in range(self.row_length):
                row = ''
                for data in metadata:
                    if data[0] == "TEXT":
                        row += self._get_word(data[1])
                    elif data[0] == 'INTEGER':
                        row += self._get_number(data[1])
                    elif data[0] == 'BOOLEAN':
                        row += self._get_bool()
                    elif data[0] == 'DATETIME':
                        row += self._get_datetime()
                if i < self.row_length - 1:
                    row += '\n'
                rows.append(row)
            file_out = spec + '_' + self._get_datetime() + '.txt'
            with open(os.path.join(data_dir, file_out), 'w') as f:
                print(f"Adding {len(rows)} rows to file")
                f.writelines(rows)

    def _get_spec(self):
        specs_files = glob(os.path.join(app_dir, "specs", "*"))

        for spec in specs_files:
            df = pd.read_csv(spec)

            spec_name = os.path.basename(spec).replace(".csv", "")
            info_list = df.to_dict("records")

            self.specs[spec_name] = [(i['datatype'], i['width']) for i in info_list]

    @staticmethod
    def _get_word(length):
        # Create random error row
        if random.randint(1, 9) == 5:
            print("There will be an error on row")
            rand_length = length + 1
            empty_space = 0
        else:
            rand_length = random.randint(max(1, length-3), length)
            empty_space = length - rand_length
        return ''.join([random.choice(string.ascii_lowercase) for _ in range(rand_length)]) + (' ' * empty_space)

    @staticmethod
    def _get_number(length):
        num_min = int('9' * (length - 1)) * -1
        num_max = int('9' * length)

        str_num = str(random.randint(num_min, num_max))
        num_length = len(str_num)

        dif_length = length - num_length

        str_num = str_num if dif_length == 0 else (' ' * dif_length) + str_num

        return str_num

    @staticmethod
    def _get_bool(bool_int=True):
        if bool_int:
            return str(random.randint(0, 1))
        else:
            return True if random.randint(0, 1) == 1 else False

    @staticmethod
    def _get_datetime():
        year = random.randint(1970, 2021)
        month = random.randint(1, 12)
        day = random.randint(1, 28)

        return str(datetime(year, month, day).date())
