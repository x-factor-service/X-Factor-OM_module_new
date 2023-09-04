import pandas as pd

def plug_in(data) :
    df = pd.DataFrame(data, columns=['today_deploy'])
    df_count = df['today_deploy'].value_counts().reset_index()
    df_count.columns = ['today_deploy', 'count']
    return df_count