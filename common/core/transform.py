from common.core.dataFrame import dataFrame

# 대시보드 상단 디스크 사용률 초과 서버, 메모리 사용률 초과 서버, 씨피유 사용률 초과 서버
def transform_donutData(sensorName) :
    transformDonutData=[]
    data = dataFrame()[dataFrame()[sensorName].str.upper() == 'YES']
    item = sensorName
    item_count = len(data)
    classification = 'dashboard_donut'
    statistics_unique = classification + '_' + item
    transformDonutData.append([statistics_unique, classification, item, item_count])
    return transformDonutData

# 대시보드 상단 OS 설치현황, 유/무선 연결 현황, 물리/가상 자산 현황(전처리 포함)
def transform_pieData(sensorName):
    transformPieData = []
    valueCounts = dataFrame()[sensorName].value_counts()
    preprocessing_list = ['no result']
    for value, count in zip(valueCounts.index, valueCounts.values):
        if not any(excluded_value.upper() in value.upper() for excluded_value in preprocessing_list):
            classification = 'dashboard_'+ sensorName
            item = value
            statistics_unique = classification + '_' + item
            item_count = count
            transformPieData.append([statistics_unique, classification, item, item_count])
    return transformPieData

def transform_disLineData(data):
    transformDisLineData = []
    classification = 'discover_unmanaged'
    item = 'count'
    item_count = data
    transformDisLineData.append([classification, item, item_count])
