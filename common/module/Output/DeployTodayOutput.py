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
DBTNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['MS']

def plug_in(data):
    logger = logging.getLogger(__name__)
    try:
        today = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        conn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        cur = conn.cursor()

        delete_query = """DELETE FROM """ + DBTNM + """ WHERE classification = 'deploy_today'"""
        cur.execute(delete_query)

        for index, row in data.iterrows():
            unique_str = row['today_deploy'] + "_" + today
            SQ = """
                INSERT INTO """ + DBTNM + """ (
                    minutely_statistics_unique,
                    classification,
                    item,
                    item_count,
                    statistics_collection_date
                ) VALUES (%s, %s, %s, %s, %s)
            """
            values = (unique_str, "deploy_today", row['today_deploy'], row['count'], today)
            cur.execute(SQ, values)

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        logger.warning('minutely_statistics Table insert connection 실패')
        logger.warning('Error : ' + str(e))