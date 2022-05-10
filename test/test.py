from scripts.App import EtlApplication
from contextlib import contextmanager
import pandas
import sys
import os


class TestEtlApp(object):

    def __init__(self, test_country_api_url, test_gdp_csv_path, test_postgres_conn_string, test_postgres_schema,
                 test_country_df, test_gdp_df):
        self.test_country_df = test_country_df
        self.test_gdp_df = test_gdp_df
        test_data = EtlApplication(test_country_api_url, test_gdp_csv_path, test_postgres_conn_string,
                                   test_postgres_schema)
        self.passed_test_cases = []
        self.failed_test_cases = []
        self.test_cases_ran = []

        @contextmanager
        def suppress_stdout():
            with open(os.devnull, "w") as devnull:
                old_stdout = sys.stdout
                sys.stdout = devnull
                try:
                    yield
                finally:
                    sys.stdout = old_stdout

        with suppress_stdout():
            test_extract_output = test_data.extract_input_data()
            test_transform_output = test_data.transform_input_data()

        self.test_extract_output = test_extract_output
        self.test_transform_output = test_transform_output

    def test_column_country_extract(self):
        test_country_actual_col = list(self.test_extract_output[0].columns)
        test_country_expected_col = list(self.test_country_df.columns)
        test_country_actual_col.sort()
        test_country_expected_col.sort()
        self.test_cases_ran.append('test_column_country_extract')
        try:
            assert test_country_actual_col == test_country_expected_col
        except Exception as e:
            self.failed_test_cases.append('test_column_country_extract')
            print('1. test_country_extract test case failed !!!\n')
            print(e)
        else:
            print('test_country_extract test case passed !!!\n')
            self.passed_test_cases.append('test_column_country_extract')

    def test_column_gdp_extract(self):
        test_gdp_actual_col = list(self.test_extract_output[1].columns)[:-1]
        test_gdp_expected_col = list(self.test_gdp_df.columns)
        test_gdp_actual_col.sort()
        test_gdp_expected_col.sort()
        self.test_cases_ran.append('test_column_gdp_extract')
        try:
            assert test_gdp_actual_col == test_gdp_expected_col
        except Exception as e:
            self.failed_test_cases.append('test_column_gdp_extract')
            print('test_column_gdp_extract test case failed !!!\n')
            print(e)
        else:
            print('test_column_gdp_extract test case passed !!!\n')
            self.passed_test_cases.append('test_column_gdp_extract')

    def test_country_value_extract(self):
        self.test_cases_ran.append('test_country_value_extract')
        try:
            assert str(self.test_country_df.country_id.item()) == str(self.test_extract_output[0].country_id.item())
            assert str(self.test_country_df.iso2_code.item()) == str(self.test_extract_output[0].iso2_code.item())
            assert str(self.test_country_df.country_name.item()) == str(self.test_extract_output[0].country_name.item())
            assert str(self.test_country_df.capital_city.item()) == str(self.test_extract_output[0].capital_city.item())
            assert str(self.test_country_df.longitude.item()) == str(self.test_extract_output[0].longitude.item())
            assert str(self.test_country_df.latitude.item()) == str(self.test_extract_output[0].latitude.item())
            assert str(self.test_country_df.region_id.item()) == str(self.test_extract_output[0].region_id.item())
            assert str(self.test_country_df.region_iso2code.item()) == str(
                self.test_extract_output[0].region_iso2code.item())
            assert str(self.test_country_df.region_value.item()) == str(self.test_extract_output[0].region_value.item())
            assert str(self.test_country_df.admin_region_id.item()) == str(
                self.test_extract_output[0].admin_region_id.item())
            assert str(self.test_country_df.admin_region_iso2code.item()) == \
                   str(self.test_extract_output[0].admin_region_iso2code.item())
            assert str(self.test_country_df.admin_region_value.item()) == str(
                self.test_extract_output[0].admin_region_value.item())
            assert str(self.test_country_df.income_level_id.item()) == str(
                self.test_extract_output[0].income_level_id.item())
            assert str(self.test_country_df.income_level_iso2code.item()) == \
                   str(self.test_extract_output[0].income_level_iso2code.item())
            assert str(self.test_country_df.income_level_value.item()) == str(
                self.test_extract_output[0].income_level_value.item())
            assert str(self.test_country_df.lending_type_id.item()) == str(
                self.test_extract_output[0].lending_type_id.item())
            assert str(self.test_country_df.lending_type_iso2code.item()) == \
                   str(self.test_extract_output[0].lending_type_iso2code.item())
            assert str(self.test_country_df.lending_type_value.item()) == str(
                self.test_extract_output[0].lending_type_value.item())
        except Exception as e:
            self.failed_test_cases.append('test_country_value_extract')
            print('test_country_value_extract test case failed !!!\n')
            print(e)
        else:
            print('test_country_value_extract test case passed !!!\n')
            self.passed_test_cases.append('test_country_value_extract')

    def test_gdp_value_extract(self):
        self.test_cases_ran.append('test_gdp_value_extract')
        try:
            assert str(self.test_gdp_df.country_code.item()) == str(self.test_extract_output[1].country_code.item())
            assert str(self.test_gdp_df.country_name.item()) == str(self.test_extract_output[1].country_name.item())
            assert str(self.test_gdp_df.indicator_name.item()) == str(self.test_extract_output[1].indicator_name.item())
            assert str(self.test_gdp_df.indicator_code.item()) == str(self.test_extract_output[1].indicator_code.item())
            assert str(self.test_gdp_df.year_2019.item()) == str(self.test_extract_output[1].year_2019.item())
            assert str(self.test_gdp_df.year_2020.item()) == str(self.test_extract_output[1].year_2020.item())
            assert str(self.test_gdp_df.year_2021.item()) == str(self.test_extract_output[1].year_2021.item())
            assert str(self.test_gdp_df.year_2022.item()) == str(self.test_extract_output[1].year_2022.item())
            assert str(self.test_gdp_df.year_2023.item()) == str(self.test_extract_output[1].year_2023.item())
        except Exception as e:
            self.failed_test_cases.append('test_gdp_value_extract')
            print('test_gdp_value_extract test case failed !!!\n')
            print(e)
        else:
            print('test_gdp_value_extract test case passed !!!\n')
            self.passed_test_cases.append('test_gdp_value_extract')

    def test_column_transform(self):
        test_country_fact_col = list(self.test_country_df[['country_id', 'region_id', 'admin_region_id',
                                                           'income_level_id', 'lending_type_id']].columns)
        test_country_details_col = list(self.test_country_df[['country_id', 'iso2_code', 'country_name', 'capital_city',
                                                              'longitude', 'latitude', 'region_id']].columns)
        test_country_region_col = list(self.test_country_df[['region_id', 'region_iso2code', 'region_value']].columns)
        test_country_admin_region_col = list(self.test_country_df[
                                                 ['admin_region_id', 'admin_region_iso2code',
                                                  'admin_region_value']].columns)
        test_country_income_level_col = list(self.test_country_df[
                                                 ['income_level_id', 'income_level_iso2code',
                                                  'income_level_value']].columns)
        test_country_lending_type_col = list(self.test_country_df[
                                                 ['lending_type_id', 'lending_type_iso2code',
                                                  'lending_type_value']].columns)

        test_transform_actual_col = [list(self.test_transform_output[2].columns), list(
            self.test_transform_output[3].columns),
                                     list(self.test_transform_output[4].columns), list(
                self.test_transform_output[5].columns),
                                     list(self.test_transform_output[6].columns), list(
                self.test_transform_output[7].columns)
                                     ]
        test_transform_expected_col = [test_country_fact_col, test_country_details_col, test_country_region_col,
                                       test_country_admin_region_col, test_country_income_level_col,
                                       test_country_lending_type_col
                                       ]
        self.test_cases_ran.append('test_column_transform')
        try:
            assert test_transform_actual_col == test_transform_expected_col
        except Exception as e:
            self.failed_test_cases.append('test_column_transform')
            print('test_column_transform test case failed !!!\n')
            print(e)
        else:
            print('test_column_transform test case passed !!!\n')
            self.passed_test_cases.append('test_column_transform')

    def test_execution_status(self):
        print('------------------- Total Test cases ran : ' + str(len(self.test_cases_ran))
              + '; Total Passed Test cases : ' + str(len(self.passed_test_cases))
              + '; Total Failed Test cases : ' + str(len(self.failed_test_cases)) + ' -------------------')


if __name__ == "__main__":
    '''Test Input parameters for the actual application'''
    country_api_url = "http://api.worldbank.org/v2/country/br?format=json"
    gdp_csv_path = "D:\PycharmProjects\Mudano\/test\/test_GEPData.csv"
    postgres_conn_string = "postgresql+psycopg2://postgres:site4POND@localhost:5432/test"
    postgres_schema = 'test'

    '''Expected Output test data '''
    country_df = pandas.DataFrame({"country_id": "BRA", "iso2_code": "BR", "country_name": "Brazil",
                                   "region_id": "LCN", "region_iso2code": "ZJ",
                                   "region_value": "Latin America & Caribbean ",
                                   "admin_region_id": "LAC", "admin_region_iso2code": "XJ",
                                   "admin_region_value": "Latin America & Caribbean (excluding high income)",
                                   "income_level_id": "UMC", "income_level_iso2code": "XT",
                                   "income_level_value": "Upper middle income", "lending_type_id": "IBD",
                                   "lending_type_iso2code": "XF", "lending_type_value": "IBRD",
                                   "capital_city": "Brasilia", "longitude": "-47.9292", "latitude": "-15.7801"},
                                  index=[0])
    gdp_df = pandas.DataFrame({"country_name": "Brazil", "country_code": "BRA",
                               "indicator_name": "GDP growth, constant (average 2010-19 prices and exchange rates)",
                               "indicator_code": "NYGDPMKTPKDZ", "year_2019": "1.2", "year_2020": "-3.9",
                               "year_2021": "4.9", "year_2022": "", "year_2023": "2.7"}, index=[0])
    gdp_df = gdp_df.mask(gdp_df == '')

    init_test = TestEtlApp(country_api_url, gdp_csv_path, postgres_conn_string, postgres_schema, country_df, gdp_df)
    init_test.test_column_country_extract()
    init_test.test_column_gdp_extract()
    init_test.test_country_value_extract()
    init_test.test_gdp_value_extract()
    init_test.test_column_transform()
    init_test.test_execution_status()
