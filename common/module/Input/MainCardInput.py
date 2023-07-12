from datetime import datetime, timedelta
import psycopg2
import requests
import json
import logging
from common.input.Session import plug_in as session
with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())

APIURL = SETTING['CORE']['Tanium']['INPUT']['API']['URL']
CSP = SETTING['CORE']['Tanium']['INPUT']['API']['PATH']['Sensor']

PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()


def plug_in():
    logger = logging.getLogger(__name__)
    try:
        SK = session()
        CSID = SETTING['CORE']['Tanium']['INPUT']['API']['SensorID']['MAINCARD']
        CSH = {'session': SK}
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

def plug_in_DB():
    logger = logging.getLogger(__name__)
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        DBHOST = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['PORT']
        DBNM = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['USER']
        DBPWD = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['PWD']
        DBTNM = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['TNM']['MC']
        CMT = SETTING['CORE']['Tanium']['CYCLE']['MINUTELY']['TIME']
        minutes_ago = (datetime.today() - timedelta(minutes=CMT/60)).strftime("%Y-%m-%d %H:%M:%S")
        DL = []
        selectConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        selectCur = selectConn.cursor()
        SQ = """
                        select 
                            os_platform
                        from  
                            """ + DBTNM + """ 
                        where 
                            to_char(collection_date , 'YYYY-MM-DD HH24:MI:SS') >= '"""+minutes_ago+"""'
             """
        selectCur.execute(SQ)
        selectRS = selectCur.fetchall()
        DATA_list = enumerate(selectRS)
        for index, RS in DATA_list:
        # for RS in selectRS:
            DL.append(RS)
        logger.info('main_card Table Select connection - 성공')
        return len(DL)
    except Exception as e:
        logger.warning('main_card Table Select connection 실패')
        logger.warning('Error : ' + str(e))