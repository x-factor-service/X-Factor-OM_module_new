from tqdm import tqdm
import logging
import json

def plug_in(data) :
    logger = logging.getLogger(__name__)
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()
        DL = []

        if PROGRESS == 'true' :
            DATA_list = tqdm( range(len(data.computer_id)),
                                total=len(data.computer_id),
                                desc='Usage')
        else :
            DATA_list = range(len(data.computer_id))

        for c in DATA_list:
        # for c in range(len(data.computer_id)):
            if data.disk_used_space[c][0].isdigit() and data.disk_total_space[c][0].isdigit() :
                driveUsage = round(int(data.disk_used_space[c][0]) / int(data.disk_total_space[c][0]) * 100, 1)
            else :
                driveUsage = 'unconfirmed'

            if data.ram_use_size[c].isdigit() and data.ram_total_size[c].isdigit() :
                ramUsage = round(int(data.ram_use_size[c]) / int(data.ram_total_size[c]) * 100, 1)
            else :
                ramUsage = 'unconfirmed'

            if type(data.cup_consumption[c]) == float :
                cpuUsage = round(data.cup_consumption[c],1)
            elif type(data.cup_consumption[c]) == int :
                cpuUsage = data.cup_consumption[c]
            elif type(data.cup_consumption[c]) == str :
                cpuUsage = 'unconfirmed'
            else :
                cpuUsage = 'unconfirmed'

            DL.append([data.computer_id[c], str(driveUsage), str(ramUsage), str(cpuUsage)])
        logger.info('Usage.py - 성공')
        return DL
    except Exception as e:
        logger.warning('Usage.py - Error 발생')
        logger.warning('Error : {}'.format(str(e)))
