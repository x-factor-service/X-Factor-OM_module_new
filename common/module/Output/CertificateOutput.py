from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import json
import logging
from tqdm import tqdm


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
        DBTNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['CRT']
        DST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['DS']

        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        insertDate = yesterday + " 23:59:59"

        insertConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        if type == 'list' :
            IQ = """
               INSERT INTO """ + DBTNM + """ (
                   computer_id, computer_name, os, ip, crt_name, crt_expire_date, collection_date
               ) VALUES (
                   %s, %s, %s, %s, %s, %s, '""" + insertDate + """'
               )                                                          
                """
            datalen = len(data.computer_id)
            DATA_list = range(datalen)
            for i in DATA_list:
                CI = data.computer_id[i]
                CN = data.computer_name[i]
                OS = data.os[i]
                IPA = data.ip[i]
                CRTN = data.crt_name[i]
                CRTED = data.crt_expire_date[i]
                dataList = CI, CN, OS, IPA, CRTN, CRTED
                insertCur.execute(IQ, (dataList))
        elif type == 'statistics':
            datalen = len(data.computer_id)
            DATA_list = range(datalen)
            CN = []
            CED = []
            for i in DATA_list:
                crt_expire_date = datetime.strptime(data.crt_expire_date[i], ' %m/%d/%Y %H')
                crt_expire_date_formatted = crt_expire_date.strftime('%Y/%m/%d')
                if crt_expire_date_formatted > yesterday:
                    CN.append(data.crt_name[i])
                    CED.append(crt_expire_date_formatted)
            DF = pd.DataFrame({
                'crt_name': CN,
                'crt_expire_date': CED
                })
            DFG = DF.groupby(['crt_name','crt_expire_date']).size().reset_index(name='counts')
            DFGS = DFG.sort_values(by='crt_expire_date', ascending=True).head(10)
            IQ = """
                INSERT INTO """ + DST + """ (
                   classification, item, item_count, statistics_collection_date
                ) VALUES (
                    %s, %s, %s, '""" + insertDate + """')                                                        
                            """
            datalen = len(DFGS.crt_name)
            DATA_list = range(datalen)
            for c in DATA_list:
                classification = 'certificate_list'
                item = DFGS['crt_name'].iloc[c]
                IC = DFGS['crt_expire_date'].iloc[c]
                dataList = classification, item, IC
                insertCur.execute(IQ, (dataList))
        insertConn.commit()
        insertConn.close()
        logger.info('certificate_asset Table INSERT connection - ' + '성공')
    except Exception as e:
        logger.warning('certificate_asset Table INSERT connection 실패')
        logger.warning('Error : ' + str(e))