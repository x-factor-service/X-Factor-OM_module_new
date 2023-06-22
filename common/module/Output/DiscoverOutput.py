from datetime import datetime, timedelta
from pprint import pprint

import psycopg2
import json
import logging
from tqdm import tqdm


def plug_in(data):
    logger = logging.getLogger(__name__)
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        DBHOST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PORT']
        DBNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['USER']
        DBPWD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PWD']
        DST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['DS']
        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        insertDate = yesterday + " 23:59:59"
        insertConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        IQ = """
                INSERT INTO """ + DST + """ (
                    classification,
                    item,
                    item_count,
                    statistics_collection_date
                ) VALUES (
                    %s, %s, %s, %s
                )
                """
        classification = 'discover'
        item = 'unmanagement'
        item_count = data
        insertCur.execute(IQ, (classification, item, item_count, insertDate))
        insertConn.commit()
        insertConn.close()
        logger.info('Statistics Table INSERT connection - ' + '성공')
    except ConnectionError as e:
        logger.warning('Statistics Table INSERT connection 실패')
        logger.warning('Error : ' + str(e))
