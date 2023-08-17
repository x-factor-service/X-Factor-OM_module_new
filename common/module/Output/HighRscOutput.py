from datetime import datetime
import psycopg2
import json
import logging


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
        DBTNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['HR']

        today = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        insertConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        insertCur.execute('TRUNCATE TABLE ' + DBTNM + ';')
        IQ = """
            INSERT INTO """ + DBTNM + """ (
                computer_id,
                computer_name,
                resource,
                os,
                ip,
                task_name,
                collection_date
            ) VALUES (
                %s, %s, %s, %s, %s, %s, '""" + today + """' ) """
        datalen = len(data.computer_id)
        DATA_list = range(datalen)
        for i in DATA_list:
            CI = data['computer_id'][i]
            CN = data['computer_name'][i]
            RSC = data['resource'][i]
            OS = data['os'][i]
            IPA = data['ip'][i]
            TN = data['task_name'][i].strip()
            dataList = CI, CN, RSC, OS, IPA, TN
            insertCur.execute(IQ, dataList)
        insertConn.commit()
        insertConn.close()
    except Exception as e:
        logger.warning('high_resource Table INSERT connection 실패')
        logger.warning('Error : ' + str(e))



