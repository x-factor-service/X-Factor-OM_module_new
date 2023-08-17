from datetime import datetime, timedelta
import requests
import psycopg2
import json
import logging
from common.input.Session import plug_in as session


with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())
APIURL = SETTING['CORE']['Tanium']['INPUT']['API']['URL']
CONNECT_CSP = SETTING['CORE']['Tanium']['INPUT']['API']['PATH']['Connect']
SENSOR_CSP = SETTING['CORE']['Tanium']['INPUT']['API']['PATH']['Sensor']
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

def plug_in():
    logger = logging.getLogger(__name__)
    SK = {'session': session()}
    try:
        # ----------------------------- 미관리 자산 --------------------------------
        try:
            DISCOVER_ID = SETTING['CORE']['Tanium']['INPUT']['API']['SensorID']['DISCOVER']
            DISCOVER_POST_URL = APIURL + CONNECT_CSP + DISCOVER_ID + '/runs/'
            DISCOVER_POST = requests.post(DISCOVER_POST_URL, headers=SK, verify=False)
            DISCOVER_DECODE = DISCOVER_POST.content.decode('utf-8', errors='ignore')
            DISCOVER_RUN_ID = json.loads(DISCOVER_DECODE)['id']
            DISCOVER_GET_URL = DISCOVER_POST_URL + str(DISCOVER_RUN_ID)

            while True:
                DISCOVER_GET = requests.get(DISCOVER_GET_URL, headers=SK, verify=False)
                DISCOVER_VALUE = DISCOVER_GET.content.decode('utf-8', errors='ignore')
                source_row_count = json.loads(DISCOVER_VALUE)['sourceRowCount']

                logger.info('Tanium API Sensor 호출 성공')
                logger.info('Sensor ID: ' + str(DISCOVER_RUN_ID))
                if source_row_count != 0:
                    break
            DISCOVER_RESULT = str(json.loads(DISCOVER_VALUE)['sourceRowCount'])
        except:
            logger.warning('Report Discover API Sensor 호출 Error 발생')
            logger.warning('Sensor ID : ' + str(DISCOVER_ID))

        # ------------------------------ 예상 유휴 자산 -----------------------------
        try:
            SQ = """
                                select 
                                    collection_date
                                from  
                                    """ + DBTNM + """ 
                                where 
                                    DATE(collection_date) < '""" + yesterday + """'"""
            selectCur.execute(SQ)
            selectRS = selectCur.fetchall()
            DL = []
            for row in selectRS:
                DL.append(row)
            IDLE_RESULT = len(DL)
        except:
            logger.warning('Idleasset Table INSERT connection 실패')
            logger.warning('Error : ' + str(Exception))

        # ------------------------------ IP 대역별 관리 자산 현황 --------------------
        try:
            ORID = SETTING['CORE']['Tanium']['INPUT']['API']['SensorID']['OM_REPORT']
            ORU = APIURL + SENSOR_CSP + ORID
            ORR = requests.post(ORU, headers=SK, verify=False)
            ORRT = ORR.content.decode('utf-8', errors='ignore')
            SUBNET_ISVM_RESULT = json.loads(ORRT)
        except:
            logger.warning('Report OM SUBNET ISVM API Sensor 호출 Error 발생')
            logger.warning('Sensor ID : ' + str(ORID))

        RD = {
            'DISCOVER_RESULT': DISCOVER_RESULT,
            'IDLE_RESULT': IDLE_RESULT,
            'SUBNET_ISVM_RESULT': SUBNET_ISVM_RESULT
        }
        return RD
    except:
        logger.warning('Report Input Error 발생')
