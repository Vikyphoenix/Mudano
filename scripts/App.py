import json
import argparse
from pullDataSources import *
from pushToDb import *


class EtlApplication(object):

    def __init__(self, country_fetch_api_url, gdp_csv_file_path, conn_string, schema):
        self.country_fetch_api_url = country_fetch_api_url
        self.gdp_csv_file_path = gdp_csv_file_path
        self.conn_string = conn_string
        self.schema = schema

    def extract_input_data(self) -> list:
        country_json_data = read_json_data_from_api(self.country_fetch_api_url)
        country_data_raw_df = json_to_df(json.loads(country_json_data)[1])
        country_data_raw_df.columns = ['country_id', 'iso2_code', 'country_name', 'capital_city',
                                       'longitude', 'latitude',
                                       'region_id', 'region_iso2code', 'region_value',
                                       'admin_region_id', 'admin_region_iso2code', 'admin_region_value',
                                       'income_level_id', 'income_level_iso2code', 'income_level_value',
                                       'lending_type_id', 'lending_type_iso2code', 'lending_type_value'
                                       ]
        gdp_data_raw_df = csv_to_df(self.gdp_csv_file_path)
        gdp_data_raw_df.columns = ['country_name', 'country_code', 'indicator_name', 'indicator_code',
                                   'year_2019', 'year_2020', 'year_2021', 'year_2022', 'year_2023', 'Unnamed'
                                   ]
        return [country_data_raw_df, gdp_data_raw_df]

    def transform_input_data(self) -> list:
        try:
            input_df_list = self.extract_input_data()
            country_data_raw_df = input_df_list[0]
            gdp_data_raw_df = input_df_list[1]
            country_fact_df = country_data_raw_df[['country_id', 'region_id', 'admin_region_id',
                                                   'income_level_id', 'lending_type_id']] \
                .drop_duplicates(['country_id', 'region_id', 'admin_region_id', 'income_level_id', 'lending_type_id']) \
                .mask(country_data_raw_df == '')
            country_details_df = country_data_raw_df[['country_id', 'iso2_code', 'country_name', 'capital_city',
                                                      'longitude', 'latitude', 'region_id']] \
                .drop_duplicates(['country_id', 'iso2_code', 'country_name', 'capital_city',
                                  'longitude', 'latitude', 'region_id']) \
                .mask(country_data_raw_df == '')
            country_region_df = country_data_raw_df[['region_id', 'region_iso2code', 'region_value']] \
                .drop_duplicates(['region_id', 'region_iso2code', 'region_value']) \
                .mask(country_data_raw_df == '')
            country_admin_region_df = country_data_raw_df[
                ['admin_region_id', 'admin_region_iso2code', 'admin_region_value']] \
                .drop_duplicates(['admin_region_id', 'admin_region_iso2code', 'admin_region_value']) \
                .mask(country_data_raw_df == '').dropna()
            country_income_level_df = country_data_raw_df[
                ['income_level_id', 'income_level_iso2code', 'income_level_value']] \
                .drop_duplicates(['income_level_id', 'income_level_iso2code', 'income_level_value']) \
                .mask(country_data_raw_df == '')
            country_lending_type_df = country_data_raw_df[
                ['lending_type_id', 'lending_type_iso2code', 'lending_type_value']] \
                .drop_duplicates(['lending_type_id', 'lending_type_iso2code', 'lending_type_value']) \
                .mask(country_data_raw_df == '').fillna('NA')
            gdp_cleansed_data_df = gdp_data_raw_df.drop(columns='Unnamed')
        except Exception as e:
            print('Error occurred while transforming the input source data into normalized data frames !!!')
            print(e)
            sys.exit(1)
        else:
            print('\nThe input data sources are successfully Cleansed and Normalized into data frames !!!!')
            print('\nThe data frames would be loaded into the below tables:')
            print('country_raw\ncountry_gdp\ncountry_fact\ncountry_details\n'
                  'region\nadmin_region\nincome_level\nlending_type\n')
            return [country_data_raw_df.mask(country_data_raw_df == ''),
                    gdp_cleansed_data_df.mask(gdp_cleansed_data_df == ''),
                    country_fact_df, country_details_df, country_region_df, country_admin_region_df,
                    country_income_level_df, country_lending_type_df
                    ]

    def load_output_data(self):
        output_df_list = self.transform_input_data()
        test_db_connection(self.conn_string, self.schema)
        print('Loading Dataframes to postgres DB tables under the schema ' + self.schema + '.....\n')
        write_to_db(self.conn_string, output_df_list[0], self.schema, 'country_raw')
        write_to_db(self.conn_string, output_df_list[1], self.schema, 'country_gdp')
        write_to_db(self.conn_string, output_df_list[4], self.schema, 'region')
        write_to_db(self.conn_string, output_df_list[5], self.schema, 'admin_region')
        write_to_db(self.conn_string, output_df_list[6], self.schema, 'income_level')
        write_to_db(self.conn_string, output_df_list[7], self.schema, 'lending_type')
        write_to_db(self.conn_string, output_df_list[2], self.schema, 'country_fact')
        write_to_db(self.conn_string, output_df_list[3], self.schema, 'country_details')
        # print('\nCompleted loading Dataframes to postgres DB !!!\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Country GDP Dataset')
    parser.add_argument('--country_api_url', required=False, default="http://api.worldbank.org/v2/country?format=json")
    parser.add_argument('--gdp_csv_path', required=False, default="D:\PycharmProjects\Mudano\input\GEPData.csv")
    # in real time scenario, the user name and password could be retrieved from a KMS,
    # also not using getpass() as it would not work in IDE
    parser.add_argument('--postgres_user_name', required=False, default="postgres")
    parser.add_argument('--postgres_password', required=False, default="site4POND")
    parser.add_argument('--postgres_host_name', required=False, default="localhost")
    parser.add_argument('--postgres_port', required=False, default="5432")
    parser.add_argument('--postgres_db', required=False, default="postgres")
    # the schema and table details are as per the DDL provided
    postgres_schema = 'countrystats'
    # please provide the above list of arguments at runtime while executing the application
    # Or please edit the above default values while executing the applidation.
    args = vars(parser.parse_args())

    postgres_conn_string = str(
        'postgresql+psycopg2://' + args['postgres_user_name'] + ':' + args['postgres_password'] + '@' +
        args['postgres_host_name'] + ':' + args['postgres_port'] + '/' + args['postgres_db'])
    process_country_gdp_data = EtlApplication(args['country_api_url'], args['gdp_csv_path'], postgres_conn_string, postgres_schema)
    process_country_gdp_data.load_output_data()
