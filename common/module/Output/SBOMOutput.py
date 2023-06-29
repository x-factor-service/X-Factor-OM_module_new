from datetime import datetime, timedelta
import psycopg2
import json
import logging


def plug_in(data, type):
    dataList = []
    logger = logging.getLogger(__name__)
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        DBHOST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PORT']
        DBNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['USER']
        DBPWD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PWD']
        DBTNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['SL']

        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        today = datetime.today().strftime("%Y-%m-%d")
        insertDate = yesterday + " 23:59:59"

        insertConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        if type == 'sbom_list':
            insertCur.execute('TRUNCATE TABLE ' + DBTNM + ';')
            insertCur.execute('ALTER SEQUENCE seq_sbom_list_num RESTART WITH 1;')
            IQ = """
                INSERT INTO """ + DBTNM + """ (
                    name,
                    version,
                    cpe,
                    type,
                    count,
                    sbom_collection_date
                ) VALUES (
                    %s, %s, %s, %s, %s, '""" + today + """' ) """
            for i in range(len(data)):
                NM = data['Name'][i]
                VS = data['Version'][i]
                CPE = data['CPE'][i]
                TY = data['Type'][i]
                CO = data['Count'][i]
                dataList = NM, VS, CPE, TY, CO
                insertCur.execute(IQ, dataList)
            insertConn.commit()
            insertConn.close()
            logger.info('sbom_list Table INSERT connection - ' + '성공')
        # elif type == 'statistics' :
        #     IQ = """
        #         INSERT INTO """ + DST + """ (
        #             classification,
        #             item,
        #             item_count,
        #             statistics_collection_date
        #         ) VALUES (
        #             %s, %s, %s, %s
        #         )
        #         """
        #     classification = 'idle_asset'
        #     item = 'collection_date'
        #     item_count = data
        #     #yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        #     insertCur.execute(IQ, (classification, item, item_count, insertDate))
        #     insertConn.commit()
        #     insertConn.close()
        #     logger.info('Idleasset Table INSERT connection - ' + '성공')
    except Exception as e:
        logger.warning('sbom_list Table INSERT connection 실패')
        logger.warning('Error : ' + str(e))



