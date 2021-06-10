from util import data_importer, postgresql_uploader


def main():
    dp = data_importer.DataProcessor()

    # Commented out since we're not running docker for this particular instance
    # psql_conn = postgresql_uploader.PostgresqlConn()
    for spec, df in dp.data_files.items():

        # psql_conn.df_to_psql(df, spec)
        print(spec)
        print(df)


if __name__ == "__main__":
    main()
