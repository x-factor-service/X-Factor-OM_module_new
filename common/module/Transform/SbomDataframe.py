import pandas as pd
from common.module.Input.SBOMInput import plug_in as sbom
from common.input.Session import plug_in as session
from pprint import pprint


def plug_in(type):
    SK = session()
    if type == 'list':
        data = sbom(SK, 'list')
        columns = [col["name"] for col in data["data"]["result_sets"][0]["columns"]]
        rows_data = data["data"]["result_sets"][0]["rows"]
        df_data = []
        for row in rows_data:
            row_data = []
            exclude_row = False  # Exclude this row from dataframe or not
            for item in row["data"]:
                values = [entry["text"] for entry in item]  # Extract all 'text' values from each item
                if '[current result unavailable]' in values or '[no results]' in values:
                    exclude_row = True  # Set to exclude row if '[current' or '[no' is in values
                    break  # No need to process this row further, so break from inner loop
                row_data.append(', '.join(values))  # Join the extracted values with ', '
            if not exclude_row:
                df_data.append(row_data)
        df = pd.DataFrame(df_data, columns=columns)
    elif type == 'detail':
        SK = session()
        DFL = []
        data = sbom(SK, 'detail')
        columns = ['computer_name', 'ipv4_address', 'name', 'version', 'cpe', 'type', 'path', 'count']
        for d in data:
            if d[2][0]['text'] == 'Not Scanned' or d[2][0]['text'] == 'No Packages Found':
                continue
            else:
                for i in range(len(d[4])):
                    CN = d[0][0]['text']
                    IP = d[1][0]['text']
                    NM = d[2][i]['text']
                    VS = d[3][i]['text']
                    CPE = d[4][i]['text']
                    TY = d[5][i]['text']
                    PA = d[6][i]['text']
                    CO = d[7][0]['text']
                    DFL.append([CN, IP, NM, VS, CPE, TY, PA, CO])
        df = pd.DataFrame(DFL, columns=columns)
    pd.set_option('display.expand_frame_repr', False)
    return df
