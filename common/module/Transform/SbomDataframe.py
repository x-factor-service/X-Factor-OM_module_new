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
            exclude_row = False
            for item in row["data"]:
                values = [entry["text"].lower() for entry in item]
                if any(any(substring in value for substring in ['result', 'error', 'tanium']) for value in values):
                    exclude_row = True
                    break
                row_data.append(', '.join(values))
            if not exclude_row:
                df_data.append(row_data)
        df = pd.DataFrame(df_data, columns=columns)
    elif type == 'detail':
        SK = session()
        DFL = []
        data = sbom(SK, 'detail')
        columns = ['computer_name', 'ipv4_address', 'name', 'version', 'cpe', 'type', 'path', 'count']
        for d in data:
            for i in range(len(d[4])):
                CN = d[0][0]['text'].lower()
                IP = d[1][0]['text'].lower()
                NM = d[2][i]['text'].lower()
                VS = d[4][i]['text'].lower()
                CPE = d[5][i]['text'].lower()
                TY = d[6][i]['text'].lower()
                PA = d[8][i]['text'].lower()
                CO = d[10][0]['text'].lower()

                if any('tanium' in col_value for col_value in [CN, IP, NM, VS, CPE, TY, PA, CO]):
                    continue
                if 'result' in NM or 'error' in NM or 'result' in VS or 'error' in VS:
                    continue

                DFL.append([CN, IP, NM, VS, CPE, TY, PA, CO])
        df = pd.DataFrame(DFL, columns=columns)
    pd.set_option('display.expand_frame_repr', False)
    return df

