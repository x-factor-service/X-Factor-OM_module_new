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
            CSRT = CSR.content.decode('utf-8', errors='ignore')
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
            CSRT = CSR.content.decode('utf-8', errors='ignore')
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

def plug_in_DB(type) :
    logger = logging.getLogger(__name__)
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        DBHOST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PORT']
        DBNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['USER']
        DBPWD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PWD']
        DBLIST = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['TNM']['SL']
        DBCVE = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['TNM']['CVE']

        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        selectConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        selectCur = selectConn.cursor()
        if type == 'sbom_cve_pie':
            SQ = """
                SELECT 
                    c.comp_name,
                    c.comp_ver,
                    COUNT(*) as count
                FROM 
                    sbom_cve c
                JOIN 
                    sbom_detail d 
                ON 
                    c.comp_name = d.name AND c.comp_ver = d.version
                GROUP BY 
                    c.comp_name, c.comp_ver
                limit 5;         
            """
        if type == 'sbom_cve_line':
            SQ = """
                SELECT 
                    COUNT(DISTINCT sbom_detail.ipv4_address) as total_unique_ipv4_count
                FROM 
                    sbom_cve
                JOIN 
                    sbom_detail
                ON 
                    sbom_cve.comp_name = sbom_detail.name 
                AND 
                    sbom_cve.comp_ver = sbom_detail.version;
                """
        if type == 'sbom_cve_bar':
            SQ = """
                SELECT 
                    sd.ipv4_address,
                    COUNT(*) as count
                FROM
                    sbom_cve sc
                JOIN
                    sbom_detail sd
                    ON sc.comp_name = sd.name AND sc.comp_ver = sd.version
                GROUP BY
                    sd.ipv4_address
                ORDER BY
                    count DESC
                LIMIT 5;
            """

        selectCur.execute(SQ)
        selectRS = selectCur.fetchall()
        return selectRS
    except Exception as e:
        logger.warning(type, 'Select connection 실패')
        logger.warning('Error : ' + str(e))
