from sqlalchemy import create_engine
import pandas

def write_to_db(conn_string: str, output_df: pandas.DataFrame, schema_name: str, table_name: str):

    try:
        engine = create_engine(conn_string)
        engine.execute('TRUNCATE'+ ' ' + schema_name + '.' + table_name + ' CASCADE;' )
        output_df.to_sql(table_name, con=engine, if_exists='append', schema=schema_name, index=False)
        engine.raw_connection().commit()
    except Exception as e:
        print('Loading ' + schema_name + '.' + table_name +  ' to the postgres DB failed !!!')
        print(e)
    else:
        print(schema_name + '.' + table_name +  ' has been truncated and loaded successfully')

