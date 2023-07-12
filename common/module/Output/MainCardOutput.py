from datetime import datetime, timedelta
import psycopg2
import json
import logging

logger = logging.getLogger(__name__)

with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())
DBHOST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['HOST']
DBPORT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PORT']
DBNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['NAME']
DBUNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['USER']
DBPWD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PWD']
DBTNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['MC']
MST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['MS']

def plug_in(data, type, disk_statistics=None, memory_statistics=None, os_counts=None, wired_counts=None,virtual_counts=None):
    today = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    insertDate = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    if type == 'asset':
        try:
            insertConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
            insertCur = insertConn.cursor()
            insertCur.execute('TRUNCATE TABLE ' + DBTNM + ';')

            IQ = """
                INSERT INTO """ + DBTNM + """ (
                    computer_id,
                    computer_name,
                    disk_gb3,
                    disk_used_space,
                    disk_free_space,
                    used_memory,
                    total_memory,
                    os_platform,
                    wired,
                    is_virtual,
                    cpu_consumption,
                    collection_date
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '""" + insertDate + """' ) """
            datalen = len(data.computer_id)
            DATA_list = range(datalen)
            for i in DATA_list:
                CI = data['computer_id'][i]
                CN = data['computer_name'][i]
                DG = data['disk_gb3'][i]
                DUS = data['disk_used_space'][i]
                DFS = data['disk_free_space'][i]
                UM = data['used_memory'][i]
                TM = data['total_memory'][i]
                OP = data['os_platform'][i]
                WID = data['wired'][i]
                IVI = data['is_virtual'][i]
                CPUN = data['cpu_consumption'][i]
                dataList = CI, CN, DG, DUS, DFS, UM, TM, OP, WID, IVI, CPUN
                insertCur.execute(IQ, dataList)
            insertConn.commit()
            insertConn.close()
        except Exception as e:
            logger.warning('maincard1 Table INSERT connection 실패')
            logger.warning('Error : ' + str(e))


    elif type == 'statistics':
        try:
            insertConn = psycopg2.connect(
                'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
            insertCur = insertConn.cursor()
            IQ = """
                INSERT INTO """ + MST + """ (
                    minutely_statistics_unique,
                    classification,
                    item,
                    item_count,
                    statistics_collection_date
                ) VALUES (
                    %s, %s, %s, %s, '""" + insertDate + """'
                )
                ON CONFLICT (minutely_statistics_unique) 
                DO UPDATE SET 
                    item_count = EXCLUDED.item_count, 
                    statistics_collection_date = EXCLUDED.statistics_collection_date;
                """
            # Disk usage data
            minutely_statistics_unique = 'donut_Disk_95'
            classification = 'disk_usage'
            item = 'disk95'
            item_count = int(disk_statistics)
            insertCur.execute(IQ, (minutely_statistics_unique, classification, item, item_count))

            # Memory usage data
            minutely_statistics_unique = 'donut_Memory_95'
            classification = 'memory_usage'
            item = 'memory95'
            item_count = int(memory_statistics)
            insertCur.execute(IQ, (minutely_statistics_unique, classification, item, item_count))

            # OS counts data
            for idx, row in os_counts.iterrows():
                minutely_statistics_unique = f'os_counts_{row["os_platform"]}'
                classification = 'os_counts'
                item = row["os_platform"]
                item_count = row["count"]
                insertCur.execute(IQ, (minutely_statistics_unique, classification, item, item_count))

            for idx, row in wired_counts.iterrows():
                minutely_statistics_unique = f'wired_counts_{row["wired"]}'
                classification = 'wired_counts'
                item = row["wired"]
                item_count = row["count"]
                insertCur.execute(IQ, (minutely_statistics_unique, classification, item, item_count))

                # Virtual counts data
            for idx, row in virtual_counts.iterrows():
                minutely_statistics_unique = f'virtual_counts_{row["is_virtual"]}'
                classification = 'virtual_counts'
                item = "Virtual" if row["is_virtual"] == "Yes" else "Physical"
                item_count = row["count"]
                insertCur.execute(IQ, (minutely_statistics_unique, classification, item, item_count))

            insertConn.commit()
            insertCur.close()
            insertConn.close()
        except Exception as e:
            logger.warning('maincard2 INSERT connection 실패')
            logger.warning('Error : ' + str(e))

def plug_in_DB(data):
    logger = logging.getLogger(__name__)
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        DBHOST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PORT']
        DBNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['USER']
        DBPWD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PWD']
        DBTNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['DS']
        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        insertDate = yesterday + " 23:59:59"
        insertConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        IQ = """
                    INSERT INTO """ + DBTNM + """ (
                        classification,
                        item,
                        item_count,
                        statistics_collection_date
                    ) VALUES (
                        %s, %s, %s, %s
                    )
                    """
        classification = 'mainCard_os'
        item = 'mainCard_os'
        item_count = data
        insertCur.execute(IQ, (classification, item, item_count, insertDate))
        insertConn.commit()
        insertConn.close()
        logger.info('MainCardOutput.py - daily_statistics Table INSERT connection - ' + '성공')
    except Exception as e:
        logger.warning('MainCardOutput.py - daily_statistics Table INSERT connection 실패')
        logger.warning('Error : ' + str(e))
