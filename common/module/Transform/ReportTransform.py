import pandas as pd

def plug_in(data, type) :
    DF = None
    if type == 'DISCOVER_RESULT' or type == 'IDLE_RESULT':
        DF = data[type]
    elif type == 'SUBNET_ISVM_RESULT' :
        source_data = data[type]
        columns = [col["name"] for col in source_data["data"]["result_sets"][0]["columns"]]
        rows_data = source_data["data"]["result_sets"][0]["rows"]
        df_data = []
        for row in rows_data:
            row_data = []
            exclude_row = False  # Exclude this row from dataframe or not
            for item in row["data"]:
                values = [entry["text"] for entry in item]  # Extract all 'text' values from each item
                if '[current result unavailable]' in values or '[no results]' in values or "Can not determine" in values:
                    exclude_row = True  # Set to exclude row if '[current' or '[no' is in values
                    break  # No need to process this row further, so break from inner loop
                row_data.append(', '.join(values))  # Join the extracted values with ', '
            if not exclude_row:
                df_data.append(row_data)
        df = pd.DataFrame(df_data, columns=columns)
        pd.set_option('display.expand_frame_repr', False)
        DF = df
    return DF

