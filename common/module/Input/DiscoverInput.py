import requests
import json
import logging
from common.input.Session import plug_in as session
from pprint import pprint
from time import sleep

with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())

APIURL = SETTING['CORE']['Tanium']['INPUT']['API']['URL']
CSP = SETTING['CORE']['Tanium']['INPUT']['API']['PATH']['Connect']

PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()

def plug_in():
    SK=session()
    logger = logging.getLogger(__name__)
    try:
        DISCOVER_ID = SETTING['CORE']['Tanium']['INPUT']['API']['SensorID']['DISCOVER']
        SESSION_KEY = {'session': SK}
        DISCOVER_POST_URL = APIURL + CSP + DISCOVER_ID + '/runs/'
        DISCOVER_POST = requests.post(DISCOVER_POST_URL, headers=SESSION_KEY, verify=False)
        DISCOVER_DECODE = DISCOVER_POST.content.decode('utf-8')
        DISCOVER_RUN_ID = json.loads(DISCOVER_DECODE)['id']
        DISCOVER_GET_URL = DISCOVER_POST_URL + str(DISCOVER_RUN_ID)

        while True:
            DISCOVER_GET = requests.get(DISCOVER_GET_URL, headers=SESSION_KEY, verify=False)
            DISCOVER_RESULT = DISCOVER_GET.content.decode('utf-8')
            source_row_count = json.loads(DISCOVER_RESULT)['sourceRowCount']

            logger.info('Tanium API Sensor 호출 성공')
            logger.info('Sensor ID: ' + str(DISCOVER_RUN_ID))
            if source_row_count != 0:
                break
        print(DISCOVER_RESULT)
        return DISCOVER_RESULT

    except:
        logger.warning('Tanium API Sensor 호출 Error 발생')
        logger.warning('Sensor ID : ' + str(DISCOVER_ID))

