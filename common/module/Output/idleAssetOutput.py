from datetime import datetime, timedelta
from pprint import pprint
import psycopg2
import json
import logging
from tqdm import tqdm


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
        DBTNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['IE']
        DST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['DS']

        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        insertDate = yesterday + " 23:59:59"

        insertConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        if type == 'asset' :
            IQ = """
                INSERT INTO """ + DBTNM + """ (
                    computer_id, 
                    computer_name, 
                    chassis_type, 
                    ipv_address, 
                    disk_used_space, 
                    disk_total_used_space, 
                    last_logged_in_date, 
                    collection_date
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, '""" + insertDate + """'
                )
                ON CONFLICT (computer_id)
                DO UPDATE SET
                    computer_id = excluded.computer_id,
                    computer_name = excluded.computer_name,
                    chassis_type = excluded.chassis_type,
                    ipv_address = excluded.ipv_address,
                    disk_used_space = excluded.disk_used_space,
                    disk_total_used_space = excluded.disk_total_used_space,
                    last_logged_in_date = excluded.last_logged_in_date,
                    collection_date = CASE 
                        WHEN (
                            ABS(excluded.disk_total_used_space::integer - """ + DBTNM + """.disk_total_used_space::integer) >= 100 AND 
                            """ + DBTNM + """.last_logged_in_date <> excluded.last_logged_in_date
                        ) THEN '""" + insertDate + """'
                        ELSE """ + DBTNM + """.collection_date
                    END
                WHERE 
                    """ + DBTNM + """.last_logged_in_date NOT LIKE 'TSE-Error: Error: WshShell.Exec: 지정된 파일을 찾을 수 없습니다.'
            """
            for i in range(len(data)):
                CI = data['Computer ID'][i]
                CN = data['Computer Name'][i]
                CT = data['Chassis Type'][i]
                IPA = data['Tanium Client IP Address'][i]
                DUS = data['Disk Used Space'][i]
                DUTU = data['Disk Used Space Test'][i]
                LLD = data['Last Logged In User Date'][i]
                dataList = CI, CN, CT, IPA, DUS, DUTU, LLD
                insertCur.execute(IQ, (dataList))
            insertConn.commit()
            insertConn.close()
            logger.info('Idleasset Table INSERT connection - ' + '성공')
        elif type == 'statistics' :
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
            classification = 'idle_asset'
            item = 'collection_date'
            item_count = data
            #yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
            insertCur.execute(IQ, (classification, item, item_count, insertDate))
            insertConn.commit()
            insertConn.close()
            logger.info('Idleasset Table INSERT connection - ' + '성공')
    except Exception as e:
        logger.warning('Idleasset Table INSERT connection 실패')
        logger.warning('Error : ' + str(e))



