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
        DBTNML = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['SL']
        DBTNMD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['SD']
        DBMS = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['SS']

        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        today = datetime.today().strftime("%Y-%m-%d")
        insertDate = yesterday + " 23:59:59"
        nowTime = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        insertConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        if type == 'sbom_list':
            insertCur.execute('TRUNCATE TABLE ' + DBTNML + ';')
            insertCur.execute('ALTER SEQUENCE seq_sbom_list_num RESTART WITH 1;')
            IQ = """
                INSERT INTO """ + DBTNML + """ (
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
            logger.info('sbom_list Table INSERT connection - ' + '성공')
        elif type == 'sbom_detail':
            insertCur.execute('TRUNCATE TABLE ' + DBTNMD + ';')
            insertCur.execute('ALTER SEQUENCE seq_sbom_detail_num RESTART WITH 1;')
            IQ = """
                INSERT INTO """ + DBTNMD + """ (
                    computer_name,
                    ipv4_address,
                    name,
                    version,
                    cpe,
                    type,
                    path,
                    count,
                    sbom_collection_date
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, '""" + today + """' ) """
            for i in range(len(data.computer_name)):
                CN = data.computer_name[i]
                IP = data.ipv4_address[i]
                NM = data.name[i]
                VS = data.version[i]
                CPE = data.cpe[i]
                TY = data.type[i]
                PA = data.path[i]
                CO = '1'
                dataList = CN, IP, NM, VS, CPE, TY, PA, CO
                insertCur.execute(IQ, dataList)
        elif type == 'cve_statistics':
            insertCur.execute("DELETE FROM " + DBMS + " WHERE classification = 'sbom_cve'")
            for _, row in data.iterrows():
                query = """
                    INSERT INTO """ + DBMS + """ (sbom_statistics_unique, classification, item, item_count, statistics_collection_date) 
                    VALUES (%s, %s, %s, %s, %s)
                """
                insertCur.execute(query, (row['sbom_statistics_unique'], row['classification'], json.dumps(row['item'], ensure_ascii=False), row['count'], nowTime))
        elif type == 'sbom_statistics':
            insertCur.execute("DELETE FROM " + DBMS + " WHERE classification = 'sbom_cpe'")
            for _, row in data.iterrows():
                query = """
                    INSERT INTO """ + DBMS + """ (sbom_statistics_unique, classification, item, item_count, statistics_collection_date) 
                    VALUES (%s, %s, %s, %s, %s)
                """
                insertCur.execute(query, (row['sbom_statistics_unique'], row['classification'], json.dumps(row['item'], ensure_ascii=False), row['count'], nowTime))
        insertConn.commit()
        insertConn.close()
        logger.info('sbom statistics data INSERT connection - ' + '성공')
    except Exception as e:
        logger.warning(type + ' Table INSERT connection 실패')
        logger.warning('Error : ' + str(e))