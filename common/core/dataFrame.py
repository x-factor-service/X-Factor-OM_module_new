from common.input.api import top_donut
import pandas as pd
from pprint import pprint
def dataFrame() :
    df_data = []
    data = top_donut()
    # 데이터 추출 및 리스트에 추가
    for i in data:
        sensor_values = {}
        for column in i['node']['sensorReadings']['columns']:
            sensor_values[column['sensor']['name']] = column['values'][0]
        df_data.append(sensor_values)
    # 데이터프레임 생성
    df = pd.DataFrame(df_data)
    #print(df)
    return df
    #print(len(df.index))      #총 자산 수





    # data = top_donut()[0]['node']['sensorReadings']['columns']
    # #pprint(data)
    # df = pd.DataFrame(data={'values': [d['values'][0] for d in data]}, index=[d['sensor']['name'] for d in data]).transpose()
    # print(df)

