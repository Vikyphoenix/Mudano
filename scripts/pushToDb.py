from sqlalchemy import create_engine
import pandas
import sys

def test_db_connection(conn_string: str, schema_name: str):
    try:
        engine = create_engine(conn_string)
        schema_check = engine.execute("SELECT count(schema_name) FROM information_schema.schemata WHERE schema_name = '" + schema_name + "';")
        if schema_check.first()[0] == 0:
            raise ('The schema ' + schema_name + 'does not exist in the provided database, Please create the tables and schema in Postgres')
        else:
            pass
    except Exception as e:
        print('Connection to the target postgres DB and ' + schema_name + ' schema fetch failed !!!')
        print(e)
        sys.exit(1)
    else:
        print('Connected to the target postgres DB and ' + schema_name + ' schema fetch sucessful !!!')


def write_to_db(conn_string: str, output_df: pandas.DataFrame, schema_name: str, table_name: str):

    try:
        engine = create_engine(conn_string)
        engine.execute('TRUNCATE'+ ' ' + schema_name + '.' + table_name + ' CASCADE;' )
        output_df.to_sql(table_name, con=engine, if_exists='append', schema=schema_name, index=False)
        engine.raw_connection().commit()
    except Exception as e:
        print('Loading ' + schema_name + '.' + table_name +  ' to the postgres DB failed !!!')
        print(e)
        sys.exit(1)
    else:
        print(schema_name + '.' + table_name +  ' has been truncated and loaded successfully')

