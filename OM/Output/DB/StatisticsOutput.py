from datetime import datetime, timedelta
import psycopg2
import json
import logging
from tqdm import tqdm

with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())
DBHOST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['HOST']
DBPORT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PORT']
DBNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['NAME']
DBUNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['USER']
DBPWD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PWD']
MST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['MS']
DST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['DS']
MSSIT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['MSSI']
PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()

def plug_in(data, cycle):
    logger = logging.getLogger(__name__)
    try:
        if cycle == 'minutely':
            TNM = MST
            insertDate = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        elif cycle == 'daily':
            TNM = DST
            yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
            insertDate = yesterday + " 23:59:59"
        elif cycle == 'minutely_session_ip':
            TNM = MSSIT
            insertDate = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        insertConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        if cycle == 'minutely':
            IQ = """
                INSERT INTO """ + TNM + """ (
                    minutely_statistics_unique, classification, item, item_count, statistics_collection_date
                ) VALUES (
                    %s, %s, %s, %s, '""" + insertDate + """'
                )
                ON CONFLICT (minutely_statistics_unique)
                DO UPDATE SET
                    classification = excluded.classification, 
                    item = excluded.item, 
                    item_count = excluded.item_count,
                    statistics_collection_date = '""" + insertDate + """'                                                                
            """
        elif cycle == 'daily':
            IQ = """
                INSERT INTO """ + TNM + """ (
                    classification, item, item_count, statistics_collection_date
                ) VALUES (
                    %s, %s, %s, '""" + insertDate + """')"""
        elif cycle == 'minutely_session_ip':
            IQ = """
                INSERT INTO """ + TNM + """ (
                    minutely_statistics_session_ip_unique, classification, item, item_count, statistics_collection_date
                ) VALUES (
                    %s, %s, %s, %s, '""" + insertDate + """'
                )
                ON CONFLICT (minutely_statistics_session_ip_unique)
                DO UPDATE SET
                    classification = excluded.classification, 
                    item = excluded.item, 
                    item_count = excluded.item_count,
                    statistics_collection_date = '""" + insertDate + """'                                                                
            """
        datalen = len(data.classification)
        
        if PROGRESS == 'true' :
            DATA_list = tqdm(range(datalen),
                            total=datalen,
                            desc='OP_DB_ST_{}'.format(cycle))
        else :
            DATA_list = range(datalen)
        for i in DATA_list:
            classification = data.classification[i]
            item = data.item[i]
            IC = str(data.item_count[i])
            if cycle == 'minutely':
                MSU = data.minutely_statistics_unique[i]
                dataList = MSU, classification, item, IC
            elif cycle == 'daily':
                dataList = classification, item, IC
            elif cycle == 'minutely_session_ip':
                MSUJ = data.minutely_statistics_session_ip_unique[i]
                dataList = MSUJ, classification, item, IC

            insertCur.execute(IQ, (dataList))
        insertConn.commit()
        insertConn.close()
        logger.info('Statistics Table INSERT connection - ' + cycle + '성공')
    except ConnectionError as e:
        logger.warning('Statistics Table INSERT connection 실패')
        logger.warning('Error : ' + str(e))

def delete(cycle):
    logger = logging.getLogger(__name__)
    try:
        current_date = datetime.now()
        one_week_ago = current_date - timedelta(weeks=1)
        deleteDate = one_week_ago.strftime('%Y-%m-%d')

        insertConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()

        if cycle == 'minutely_delete':
            IQ = """
                DELETE FROM minutely_statistics 
                WHERE
                    classification = 'running_service'
                    or
                    classification = 'installed_applications'
                    or
                    classification = 'session_ip_computer_name'
                """

        elif cycle == 'minutely_sessionIP_delete':
            IQ = """
                DELETE FROM minutely_statistics_session_ip 
                """

        elif cycle == 'daily_delete':
            IQ = """
                DELETE FROM daily_statistics 
                WHERE
                    classification ='session_ip'
                    or 
                    classification = 'running_service'
                    or
                    classification = 'installed_applications'
                    AND statistics_collection_date <= '""" + deleteDate + """'
                """

        insertCur.execute(IQ)
        insertConn.commit()
        insertConn.close()
        logger.info('Statistics Table DELETE connection - ' + cycle + '성공')
    except ConnectionError as e:
        logger.warning('Statistics Table DELETE connection 실패')
        logger.warning('Error : ' + str(e))

def session_ip_select():
    logger = logging.getLogger(__name__)
    try:
        DL = []
        current_date = datetime.now()
        fiveMinutesAgo = (datetime.today() - timedelta(minutes=11)).strftime("%Y-%m-%d %H:%M:%S")
        selectConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        selectCur = selectConn.cursor()
        SQ = """
                select
                    classification, item, item_count, coalesce (minutely_statistics_list.computer_name, 'Virtual/Service IP') as computer_name
                from
                    minutely_statistics_session_ip
                left join minutely_statistics_list
                on split_part(minutely_statistics_session_ip.item,':',1) = minutely_statistics_list.ipv_address
                where
                    classification = 'session_ip' and item != 'NO'
                and
                    statistics_collection_date >= '""" + fiveMinutesAgo + """'
                order by
                    item_count::INTEGER desc limit 3
                """

        selectCur.execute(SQ)
        selectRS = selectCur.fetchall()

        for RS in selectRS:
            DL.append(RS)
        logger.info('session_ip + Computer_name Statistics Table SELECT - 성공')
        session_ip_insert(DL)
        return DL

    except ConnectionError as e:
        logger.warning(
            'session_ip + Computer_name Statistics Table SELECT 실패 Common/Output/DB/Postgresql/Tanium/Dashboard/Statistics.py')
        logger.warning('Error : ' + str(e))

def session_ip_insert(data):
    logger = logging.getLogger(__name__)
    insertDate = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    try:
        insertConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        IQ = """
                INSERT INTO minutely_statistics (
                    minutely_statistics_unique, classification, item, item_count, statistics_collection_date
                ) VALUES (
                    %s, %s, %s, %s, '""" + insertDate + """'
                )
                ON CONFLICT (minutely_statistics_unique)
                DO UPDATE SET
                    classification = excluded.classification, 
                    item = excluded.item, 
                    item_count = excluded.item_count,
                    statistics_collection_date = '""" + insertDate + """' 
                """
        datalen = len(data)
        DATA_list = range(datalen)
        for i in DATA_list:
            classification = data[i][0] + '_computer_name'
            item = data[i][1] + '_' +data[i][3]
            IC = data[i][2]
            MSU = str(classification) + str(item)
            dataList = MSU, classification, item, IC

            insertCur.execute(IQ, (dataList))

        insertConn.commit()
        insertConn.close()
        logger.info('session_ip + Computer_name Statistics Table INSERT - 성공')
    except ConnectionError as e:
        logger.warning('session_ip + Computer_name Statistics Table INSERT 실패 Common/Output/DB/Postgresql/Tanium/Dashboard/Statistics.py')
        logger.warning('Error : ' + str(e))