import requests
import json
import logging

with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())

APIURL = SETTING['CORE']['Tanium']['INPUT']['API']['URL']
CSP = SETTING['CORE']['Tanium']['INPUT']['API']['PATH']['Sensor']

PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()


def plug_in(sessionKey):
    logger = logging.getLogger(__name__)
    try:
        CSID = SETTING['CORE']['Tanium']['INPUT']['API']['SensorID']['HIGHCPUPROC']
        CSH = {'session': sessionKey}
        CSU = APIURL + CSP + CSID
        CSR = requests.post(CSU, headers=CSH, verify=False)
        CSRT = CSR.content.decode('utf-8')
        CSRJ = json.loads(CSRT)
        CSRJD = CSRJ['data']
        dataList = []
        DATA_list = CSRJD['result_sets'][0]['rows']

        for d in DATA_list:  # index 제거
            DL = []
            for i in d['data']:
                DL.append(i)
            dataList.append(DL)
        logger.info('Tanium API Sensor 호출 성공')
        logger.info('Sensor ID : ' + str(CSID))
        return dataList
    except:
        logger.warning('Tanium API Sensor 호출 Error 발생')
        logger.warning('Sensor ID : ' + str(CSID))