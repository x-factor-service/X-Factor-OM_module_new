from datetime import datetime
import json
import logging
import psycopg2

with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())
DBHOST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['HOST']
DBPORT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PORT']
DBNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['NAME']
DBUNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['USER']
DBPWD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PWD']
DBTNM = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['TNM']['AL']

def plug_in():
    logger = logging.getLogger(__name__)
    try:
        today = datetime.today().strftime("%Y-%m-%d")
        selectConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        selectCur = selectConn.cursor()
        SQ = """
            select
                package
            from
                """ + DBTNM + """
            where
                DATE(log_collection_date) = '""" + today + """'
        """
        selectCur.execute(SQ)
        selectRS = selectCur.fetchall()
        return selectRS

    except Exception as e:
        logger.warning('action_log Table select connection 실패')
        logger.warning('Error : ' + str(e))