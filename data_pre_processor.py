try:
    from matplotlib import pyplot as plt
    from prettytable import PrettyTable
    import numpy as np
    import pandas as pd
except ModuleNotFoundError as err:
    print(f"[-] Could not find module: {err}")
    print(f"[-] Try pip install {err} to install the package")


def get_data_types(dataframe_object):
    """
    returns the data type each column by looping through each column's
    first element and checking their data type.
    This is required because the inbuilt function info (deprecated now)
    is not able to distinguish between the complex data types

    Parameters:
    dataframe_object: A dataframe object i.e. something which was created using
    pandas.read_csv, pandas.read_excel etc.
    """
    table = PrettyTable()
    table.field_names = ["Column", "Non-Null Count", "Dtype"]

    for column in dataframe_object.columns:
        non_null_count = int(len(dataframe_object) -
                             dataframe_object[column].isna().sum())
        data_type = type(dataframe_object[column].iloc[0])
        if data_type == None:
            i = 0
            while i < len(dataframe_object):
                data_type = type(dataframe_object[column].iloc[i])
                if data_type != None:
                    break
                i += 1
        table.add_row([column, non_null_count, data_type])

    print(table)


def lat_lon_str_parser(column, data):
    """
    The data contained in the latitude and longitude columns are in array which 
    gets saved as string. 
    This function parses to string and finds the most frequent values within the
    array to return a single value of latitude or longitude. This can be used
    to replace the string of array in the latitude or longitude by a single value.

    Example of data: "['0.000000', '0.000000', '0.000000', '0.000000']"
    Parameters
    column: The column of the dataframe which needs to be parsed
    """
    lat_list = []
    try:
        if type(data[column].iloc[0]) != str:
            return data[column]

        for row in range(len(data)):
            try:
                item = data[column].iloc[row]
                if len(item) < 20:
                    lat_list.append(float(item))
                else:
                    values = item.split(",")
                    res = float(
                        max(set(values), key=values.count).split("'")[1])
                    lat_list.append(res)
            except AttributeError:
                lat_list.append(np.nan)

    except TypeError as err:
        print(f"[-] Type error: {err}")
        return []

    return lat_list


def sog_cog_to_num(column, data):
    """
    Some of the data collected from the IMU sensors are sent as string of array.
    We need to parse the string and extract the array with numerical values.

    Example of data: "['0.00', '0.00', '0.00', '0.00', '0.00', '0.00']"
    Parameters
    column: The column of the dataframe which needs to be parsed.
    """
    return_list = []
    for row in range(len(data)):
        row_list = []
        for i in range(1000):
            try:
                num = float(data[column].iloc[row].split(
                    "[")[1].split("]")[0].split(",")[i].split("'")[1])
                row_list.append(num)
            except AttributeError:
                row_list.append(np.nan)

        return_list.append(row_list)

    return return_list


def label_parser(column, data):
    """
    The label of data collected is in string format. This function parses
    the string into individual string values. The output is an array of string 
    values which tells the road condition.

    Example of data: "['Roughroad', 'Roughroad', 'Roughroad', 'Roughroad']"
    Parameters
    column: The column of the dataframe which needs to be parsed.
    """
    return_list = []
    for row in range(len(data)):
        row_list = []
        for i in range(1000):
            try:
                value = data[column][row].split("[")[1].split("]")[
                    0].split(",")[i].split("'")[1]
                row_list.append(value)
            except AttributeError:
                row_list.append(np.nan)

        return_list.append(row_list)

    return return_list


def str_to_num_parser(column, data):
    """
    Parses the data sent as string of array.

    Example of data: '[-3696, -1972, 4984, -3248]'

    Parameters
    column: column of data which needs to be parsed.
    """
    return_list = []
    for row in range(len(data)):
        value_list = data[column].iloc[row].split(
            "[")[1].split("]")[0].split(",")
        row_list = [float(value) for value in value_list]
        return_list.append(row_list)

    return return_list


def pre_processor(df,
                  str_arrays=['SogAcc', 'CogAcc', 'AcX', 'AcY',
                              'AcZ', 'GcX', 'GcY', 'GcZ', 'Tmp',
                              'Time', 'B', 'A'],
                  label_array=['Label'],
                  lat_lon=[],
                  cog_sog=[],
                  export=False,
                  export_name=None):
    """
    This function takes in the dataframe which needs to be processed along with list of columns 
    which need pre-processing. Different columns can have different pre-processing requirements 
    and based on the history of data seen, the code has functionality to process them.

    Parameters

    df: IMU data's dataframe which needs to be  processed.
    lat_lon: expects a list of columns which need specific pre-processing. Default is empty list
            but for specific datasets it can be set as the example below.
    cog_sog: expects a list of columns which need specific pre-processing. Default is empty list
            but for specific datasets it can be set as the example below.
    str_arrays: expects a list of columns which needs to be converted to array of float values. 
            The original data should be a simple string of array. In most cases the default values
            should hold good.

    Example:

    pre_processor(df,
                  lat_lon=["LonAcc", "LatAcc"],
                  cog_sog=["CogAcc", "SogAcc"],
                  str_arrays =['SogAcc',
                                 'CogAcc', 'AcX', 'AcY', 'AcZ', 'GcX', 'GcY', 'GcZ', 'Tmp', 'Time', 'B',
                                 'A'],
                  export=False,
                  export_name=None):
    """
    data = df.copy()
    cols_to_drop = ["_id"]
    for column in data.columns:
        data_points = (data[column].isna().sum()/len(data))
        if data_points > 0.50:
            cols_to_drop.append(column)
    data.drop(columns=cols_to_drop, axis=1, inplace=True)
    data.dropna(inplace=True)

    try:
        for col in lat_lon:
            print(col)
            data[col] = lat_lon_str_parser(col, data=data)

        for col in label_array:
            data[col] = label_parser(col, data=data)

        for col in cog_sog:
            print(col)
            data[col] = sog_cog_to_num(col, data=data)

        for col in str_arrays:
            print(col)
            data[col] = str_to_num_parser(col, data=data)

        if export:
            if export_name == None:
                export_name = "unnamed"
            data.to_csv(f"{export_name}.csv", index=False)
            print(f"[+] Exported {export_name}")

    except KeyError as err:
        print(f"[-] Could not find key: {err}")
        return pd.DataFrame()

    return data
