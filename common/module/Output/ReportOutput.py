from datetime import datetime, timedelta
import psycopg2
import json
import logging

def plug_in(data, type):
    logger = logging.getLogger(__name__)
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        DBHOST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PORT']
        DBNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['USER']
        DBPWD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PWD']
        RST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['RP']
        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        insertDate = yesterday + " 23:59:59"
        insertConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        if type == 'DISCOVER_RESULT':
            IQ = """
                    INSERT INTO """ + RST + """ (
                        classification,
                        item,
                        item_count,
                        statistics_collection_date
                    ) VALUES (
                        %s, %s, %s, %s
                    )
                    """
            classification = 'daily_om'
            item = 'unmanagement'
            item_count = data
            insertCur.execute(IQ, (classification, item, item_count, insertDate))
        if type == 'IDLE_RESULT':
            IQ = """
                    INSERT INTO """ + RST + """ (
                        classification,
                        item,
                        item_count,
                        statistics_collection_date
                    ) VALUES (
                        %s, %s, %s, %s
                    )
                    """
            classification = 'daily_om'
            item = 'idle'
            item_count = data
            insertCur.execute(IQ, (classification, item, item_count, insertDate))
        if type == 'SUBNET_ISVM_RESULT':
            IQ = """
                    INSERT INTO """ + RST + """ (
                        classification,
                        item,
                        item_count,
                        statistics_collection_date
                    ) VALUES (
                        %s, %s, %s, %s
                    )
                    """
            for index, row in data.iterrows():
                item = row['Tanium Client Subnet']
                item_count = row['Count']
                if row['Is Virtual'] == 'Yes':
                    classification = 'daily_om_vm'
                elif row['Is Virtual'] == 'No':
                    classification = 'daily_om_pm'
                insertCur.execute(IQ, (classification, item, item_count, insertDate))
        insertConn.commit()
        insertConn.close()
        logger.info('report_statistics Table INSERT connection - ' + '성공')
    except ConnectionError as e:
        logger.warning('report_statisticse INSERT connection 실패')
        logger.warning('Error : ' + str(e))
