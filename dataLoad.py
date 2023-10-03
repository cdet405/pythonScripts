# 20231003 CD 01059 -> dataLoad
import os
import pandas as pd
from config import db_host, db_user, db_password, db_schema, db_port, db_table
from sqlalchemy import create_engine

# create mysql engine
def sql_engine():
    user = db_user
    password = db_password
    host = db_host
    port = db_port
    schema = db_schema
    engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(user, password, host, port, schema))
    return engine

# fetch / clean file
def data_prep():
    pd.set_option('display.max_columns', None)
    path = 'C:\\Users\\me\\Downloads\\'
    file = 'ticket01059.csv'
    file_path = os.path.join(path, file)
    try:
        df = pd.read_csv(file_path, encoding='ISO-8859-1')
        print(df.head(8).to_markdown(index=False))
        print(df.info())
    except Exception as e:
        print('\nyikes!\n\n', e,)
        df = None
    return df

# insert file to database
def data_load():
    table = db_table
    engine = sql_engine()
    df = data_prep()
    df.to_sql(name=table,
              con=engine,
              if_exists='append',
              method='multi',
              chunksize=1000,
              index=False
              )
    print(f'\ninserted data to {table}')


if __name__ == '__main__':
    print('\nstarting....\n')
    data_load()
    print('\n\ndone\n\n')
