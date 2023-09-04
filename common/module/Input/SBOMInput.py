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
        if type == 'cve_in_sbom':
            SQ = """
                    SELECT 
                        comp_name, 
                        comp_ver, 
                        cve_id, 
                        score, 
                        vuln_last_reported, 
                        number, 
                        note, 
                        solution,
                        COALESCE(
                            (SELECT SUM(CAST(count AS INTEGER)) 
                             FROM """ + DBLIST + """
                             WHERE
                             (
                                 lower(name) = lower(""" + DBCVE + """.comp_name) 
                             AND
                                 lower(version) = lower(""" + DBCVE + """.comp_ver)
                             )
                            ), 0) as total_matching_count
                    FROM """ + DBCVE + """
                    WHERE EXISTS 
                        (SELECT 1
                         FROM """ + DBLIST + """
                         WHERE
                         (
                             lower(name) = (""" + DBCVE + """.comp_name) 
                         AND
                             lower(version) = lower(""" + DBCVE + """.comp_ver)
                         )
                        )
                """
        if type == 'sbom_in_cve':
            SQ = """
                    SELECT name, version, cpe, type, count
                    FROM """ + DBLIST + """
                    WHERE EXISTS (
                        SELECT 1
                        FROM """ + DBCVE + """
                        WHERE
                        (
                            lower(""" + DBLIST + """.name) = lower(""" + DBCVE + """.comp_name)
                            AND
                            lower(""" + DBLIST + """.version) = lower(""" + DBCVE + """.comp_ver)
                        )
                    )
                """
        if type == 'sbom_cve_line':
            SQ = """
                SELECT count(distinct ipv4_address)
                    FROM sbom_detail
                    JOIN (
                        SELECT cpe
                        FROM sbom_list
                        WHERE EXISTS (
                            SELECT 1
                            FROM sbom_cve
                            WHERE
                            (
                                lower(sbom_list.name) = lower(sbom_cve.comp_name)
                                AND
                                lower(sbom_list.version) = lower(sbom_cve.comp_ver)
                            )
                        )
                    ) AS matching_cpes ON sbom_detail.cpe = matching_cpes.cpe;
                """
        if type == 'sbom_cve_bar':
            SQ = """
                SELECT ipv4_address, COUNT(*) AS count
                FROM (
                    SELECT *
                    FROM sbom_detail
                    JOIN (
                        SELECT cpe
                        FROM sbom_list
                        WHERE EXISTS (
                            SELECT 1
                            FROM sbom_cve
                            WHERE
                            (
                                lower(sbom_list.name) = lower(sbom_cve.comp_name)
                                AND
                                lower(sbom_list.version) = lower(sbom_cve.comp_ver)
                            )
                        )
                    ) AS matching_cpes ON sbom_detail.cpe = matching_cpes.cpe
                ) AS grouped_data
                GROUP BY ipv4_address
                order by count desc, ipv4_address asc
                limit 5;
            """

        selectCur.execute(SQ)
        selectRS = selectCur.fetchall()
        return selectRS
    except Exception as e:
        logger.warning(type, 'Select connection 실패')
        logger.warning('Error : ' + str(e))
