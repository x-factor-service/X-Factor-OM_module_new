import pandas as pd
from common.module.Input.idleAssetInput import plug_in as idle
from common.input.Session import plug_in as session
from pprint import pprint


def plug_in():
    SK = session()
    data = idle(SK)
    columns = [col["name"] for col in data["data"]["result_sets"][0]["columns"]]
    rows_data = data["data"]["result_sets"][0]["rows"]
    df_data = []
    #pprint(rows_data)
    for row in rows_data:
        row_data = []
        exclude_row = False
        for item in row["data"]:
            values = [entry["text"] for entry in item]
            if any(text in value for value in values for text in ["result", "Error"]):
                exclude_row = True
                break
            row_data.append(', '.join(values))
        if not exclude_row:
            df_data.append(row_data)
    df = pd.DataFrame(df_data, columns=columns)
    pd.set_option('display.expand_frame_repr', False)
    return df


#----------------전부쌓는로직------------------
# def plug_in():
#     SK = session()
#     data = idle(SK)
#     columns = [col["name"] for col in data["data"]["result_sets"][0]["columns"]]
#     rows_data = data["data"]["result_sets"][0]["rows"]
#     df_data = []
#     for row in rows_data:
#         row_data = []
#         for item in row["data"]:
#             values = [entry["text"] for entry in item]  # Extract all 'text' values from each item
#             row_data.append(', '.join(values))  # Join the extracted values with ', '
#         df_data.append(row_data)
#     df = pd.DataFrame(df_data, columns=columns)
#     pd.set_option('display.expand_frame_repr', False)
#     return df
