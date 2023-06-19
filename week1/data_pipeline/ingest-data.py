import argparse
import os
import pandas as pd
from time import time
from sqlalchemy import create_engine

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name1 = params.table_name_1
    table_name2 = params.table_name_2
    url1 = params.url_1
    url2 = params.url_2

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url1.endswith('.csv.gz'):
        csv_name1 = 'output1.csv.gz'
    else:
        csv_name1 = 'output1.csv'
    
    if url2.endswith('.csv.gz'):
        csv_name2 = 'output2.csv.gz'
    else:
        csv_name2 = 'output2.csv'

    os.system(f"wget {url1} -O {csv_name1}")
    os.system(f"wget {url2} -O {csv_name2}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter1 = pd.read_csv(csv_name1, iterator=True, chunksize=100000)
    df_iter2 = pd.read_csv(csv_name2, iterator=True, chunksize=100000)

    df1 = next(df_iter1)
    df2 = next(df_iter2)

    df1.head(n=0).to_sql(name=table_name1, con=engine, if_exists='replace')
    df2.head(n=0).to_sql(name=table_name2, con=engine, if_exists='replace')

    df1.to_sql(name=table_name1, con=engine, if_exists='append')
    df2.to_sql(name=table_name2, con=engine, if_exists='append')

    while True: 

        try:
            t_start = time()
            
            df1 = next(df_iter1)
            df2 = next(df_iter2)

         

            df1.to_sql(name=table_name1, con=engine, if_exists='append')
            df2.to_sql(name=table_name2, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break
            

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name_1', required=True, help='name of the first table where we will write the results to')
    parser.add_argument('--url_1', required=True, help='url of the first csv file')
    parser.add_argument('--table_name_2', required=True, help='name of the second table where we will write the results to')
    parser.add_argument('--url_2', required=True, help='url of the second csv file')

    args = parser.parse_args()
    main(args)