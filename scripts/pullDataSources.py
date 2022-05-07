from urllib.request import urlopen
import pandas
import sys

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)


def read_json_data_from_api(url: str) -> str:
    try:
        response = urlopen(url)
        json_lines = response.read().decode("utf-8")
    except Exception as e:
        print('Reading data from the url ' + url + 'loading to pandas Dataframe failed !!!')
        print(e)
        sys.exit(1)
    else:
        print("The json response is :"+"\n"+json_lines)
        return json_lines


def json_to_df(ip_json: list) -> pandas.DataFrame:
    try:
        json_output_df = pandas.json_normalize(ip_json)
    except Exception as e:
        print('Loading below input json to pandas Dataframe failed !!!\n' + str(ip_json))
        print(e)
        sys.exit(1)
    else:
        print("The dataframe generated from json response is :"+"\n")
        print(json_output_df)
        return json_output_df


def csv_to_df(csv_location: str) -> pandas.DataFrame:
    try:
        csv_output_df = pandas.read_csv(csv_location, header=0)
    except Exception as e:
        print('Loading below csv file to pandas Dataframe failed !!!\n' + csv_location)
        print(e)
        sys.exit(1)

    else:
        print("The dataframe generated from csv file is :"+"\n")
        print(csv_output_df)
        return csv_output_df
