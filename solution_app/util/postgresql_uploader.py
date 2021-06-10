import pandas as pd
from sqlalchemy import create_engine


class PostgresqlConn:
    def __init__(
            self,
            db_name='database',
            db_user='username',
            db_pass='secret',
            db_host='db',
            db_port='5432'
    ):
        self.db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
        # print(self.db_string)
        self.db_engine = create_engine(self.db_string)

    def df_to_psql(self, df, table_name):
        """
        accepts the Pandas DataFrame and push it to the table
        :param df: pd.DataFrame
        :param table_name: name of the table
        :return: N/A
        """
        if isinstance(df, pd.DataFrame):
            print(f"Inserting data to {table_name}")
            try:
                df.to_sql(table_name, self.db_engine, if_exists='append', index=False, chunksize=10000)
            except ImportError as IE:
                print(f"Could not push the DataFrame:\n{IE}")
        else:
            raise TypeError("Data has to be a pd.DataFrame type")

    def query_psql(self, query):
        """
        queries the postgresql
        :param query: actual query statment
        :return: list of tuples
        """
        try:
            result_set = self.db_engine.execute(query)
        except ConnectionError as CE:
            raise CE

        return [row for row in result_set]
