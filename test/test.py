from scripts.App import EtlApplication
from contextlib import contextmanager
import pandas
import sys
import os


@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


'''Test Input parameters for the actual application'''
test_country_api_url = "http://api.worldbank.org/v2/country/br?format=json"
test_gdp_csv_path = "D:\PycharmProjects\Mudano\/test\/test_GEPData.csv"
test_postgres_conn_string = "postgresql+psycopg2://postgres:site4POND@localhost:5432/test"

with suppress_stdout():
    test_data = EtlApplication(test_country_api_url, test_gdp_csv_path, test_postgres_conn_string)
    test_extract_output = test_data.extract_input_data()
    # test_transform_output= test_data.transform_input_data()


'''Expected Output test data '''
test_country_df = pandas.DataFrame({"country_id": "BRA", "iso2_code": "BR", "country_name": "Brazil",
                                    "region_id": "LCN", "region_iso2code": "ZJ",
                                    "region_value": "Latin America & Caribbean (all income levels)",
                                    "admin_region_id": "LAC", "admin_region_iso2code": "XJ",
                                    "admin_region_value": "Latin America & Caribbean (developing only)",
                                    "income_level_id": "UMC", "income_level_iso2code": "XT",
                                    "income_level_value": "Upper middle income", "lending_type_id": "IBD",
                                    "lending_type_iso2code": "XF", "lending_type_value": "IBRD",
                                    "capital_city": "Brasilia", "longitude": "-47.9292", "latitude": "-15.7801"},
                                   index=[0])
test_gdp_df = pandas.DataFrame({"country_name": "Brazil", "country_code": "BRA",
                                "indicator_name": "GDP growth, constant (average 2010-19 prices and exchange rates)",
                                "indicator_code": "NYGDPMKTPKDZ", "year_2019": "1.2", "year_2020": "-3.9",
                                "year_2021": "4.9", "year_2022": "1.4", "year_2023": "2.7"},
                               index=[1])

passed_test_cases = []
failed_test_cases = []
test_cases_ran = []


def test_column_country_extract():
    test_extract_country_col = list(test_extract_output[0].columns)
    test_country_df_col = list(test_country_df.columns)
    test_extract_country_col.sort()
    test_country_df_col.sort()
    test_cases_ran.append('test_column_country_extract')
    try:
        assert test_extract_country_col == test_country_df_col
    except Exception as e:
        failed_test_cases.append('test_column_country_extract')
        print('1. test_column_country_extract test case failed !!!\n')
        print(e)
    else:
        print('test_column_country_extract test case passed !!!\n')
        passed_test_cases.append('test_column_country_extract')


def test_column_gdp_extract():
    test_extract_gdp_col = list(test_extract_output[1].columns)[:-1]
    test_gdp_df_col = list(test_gdp_df.columns)
    test_extract_gdp_col.sort()
    test_gdp_df_col.sort()
    test_cases_ran.append('test_column_gdp_extract')
    try:
        assert test_extract_gdp_col == test_gdp_df_col
    except Exception as e:
        failed_test_cases.append('test_column_gdp_extract')
        print('test_column_column_gdp_extract test case failed !!!\n')
        print(e)
    else:
        print('test_column_column_gdp_extract test case passed !!!\n')
        passed_test_cases.append('test_column_gdp_extract')


if __name__ == "__main__":
    test_column_country_extract()
    test_column_gdp_extract()
    print('------------------- Total Test cases ran : ' + str(len(test_cases_ran)) + '; Total Passed Test cases : ' +
          str(len(passed_test_cases)) + '; Total Failed Test cases : '
          + str(len(failed_test_cases)) + ' -------------------')
    #
    # test_column_transform
    # test_value_extract
    # test_value_transform
    # test_postgres_conn_schema
    # test_table_insert
    # convert to test class
