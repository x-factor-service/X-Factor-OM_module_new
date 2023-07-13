from pprint import pprint
import pandas as pd
import requests
import json
import logging
from datetime import datetime, timedelta
import psycopg2
from tqdm import tqdm
from common.input.Session import plug_in as SK

with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())

APIURL = SETTING['CORE']['Tanium']['INPUT']['API']['URL']
CSP = SETTING['CORE']['Tanium']['INPUT']['API']['PATH']['Sensor']

PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()


def plug_in(sessionKey, type):
    logger = logging.getLogger(__name__)
    try:
        if type == 'list':
            CSID = SETTING['CORE']['Tanium']['INPUT']['API']['SensorID']['SBOM']
            CSH = {'session': sessionKey}
            CSU = APIURL + CSP + CSID
            CSR = requests.post(CSU, headers=CSH, verify=False)
            CSRC = CSR.status_code
            CSRT = CSR.content.decode('utf-8')
            CSRJ = json.loads(CSRT)
            # CSRJD = CSRJ['data']
            # dataList = []
            logger.info('Tanium API Sensor 호출 성공')
            logger.info('Sensor ID : ' + str(CSID))
            return CSRJ
        elif type == 'detail':
            CSID = SETTING['CORE']['Tanium']['INPUT']['API']['SensorID']['SBOM_DETAIL']
            CSH = {'session': sessionKey}
            CSU = APIURL + CSP + CSID
            CSR = requests.post(CSU, headers=CSH, verify=False)
            CSRC = CSR.status_code
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

def plug_in_DB() :
    logger = logging.getLogger(__name__)
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        DBHOST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PORT']
        DBNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['USER']
        DBPWD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PWD']
        DBTNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['IE']

        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        selectConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        selectCur = selectConn.cursor()
        SQ = """
                    select 
                        collection_date
                    from  
                        """ + DBTNM + """ 
                    where 
                        DATE(collection_date) < '"""+yesterday+"""'"""
        selectCur.execute(SQ)
        selectRS = selectCur.fetchall()
        DL = []
        for row in selectRS:
            DL.append(row)
        return len(DL)
    except Exception as e:
        logger.warning('Idleasset Table INSERT connection 실패')
        logger.warning('Error : ' + str(e))
