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
OD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['O']


def plug_in(data):
    logger = logging.getLogger(__name__)
    try:
        OCPO = str(data['os_chartPartOne'])
        OCPT = str(data['os_chartPartTwo'])
        OD = str(data['os_donutChartData'])
        IDL = str(data['idleDataList'])
        SBCDL = str(data['server_BChartDataList'])
        SDCD = str(data['service_donutChartData'])
        DCDL = str(data['DiskChartDataList'])
        CCDL = str(data['CpuChartDataList'])
        MCDL = str(data['MemoryChartDataList'])
        DNDL = str(data['diskNormalDataList'])
        CNDL = str(data['cpuNormalDataList'])
        MNDL = str(data['memoryNormalDataList'])
        SLCDL = str(data['server_LChartDataList'])
        WWDL = str(data['WorldMapDataList'])
        ACDL = str(data['alamCaseDataList'])
        ADCD = str(data['alarm_donutChartData'])
        VCL = str(data['vendorChartList'])
        CIDL = str(data['connectIpDataList'])
        CSDL = str(data['connectServerDataList'])
        BDL = str(data['bannerDataList'])


        insertDate = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        insertConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        insertCur = insertConn.cursor()
        IQ = """
            INSERT INTO om (
                    os_chartPartOne, os_chartPartTwo, os_donutChartData, idleDataList, server_BChartDataList, service_donutChartData, DiskChartDataList, CpuChartDataList, MemoryChartDataList, diskNormalDataList, cpuNormalDataList, memoryNormalDataList, server_LChartDataList, WorldMapDataList, alamCaseDataList, alarm_donutChartData, vendorChartList, connectIpDataList, connectServerDataList, bannerData, om_collection_date          
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,'""" + insertDate + """'
                )
        """


        dataList = OCPO, OCPT, OD, IDL, SBCDL, SDCD, DCDL, CCDL, MCDL, DNDL, CNDL, MNDL, SLCDL, WWDL, ACDL, ADCD, VCL, CIDL, CSDL, BDL
        insertCur.execute(IQ, (dataList))

        insertConn.commit()
        insertConn.close()
        logger.info('om Table INSERT connection - 성공')
    except ConnectionError as e:
        logger.warning('om Table INSERT connection 실패')
        logger.warning('Error : ' + str(e))

