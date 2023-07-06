import json
import logging
from datetime import datetime
import re
from tqdm import tqdm


def plug_in(data, dataType):
    logger = logging.getLogger(__name__)
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()
        if PROGRESS == 'true' :
            DATA_list = tqdm(range(len(data['computer_id'])),
                                total=len(data['computer_id']),
                                desc='Common_TransForm_PrePro_{}'.format(dataType),
                                )
        else :
            DATA_list = range(len(data))

        DL = []
        for c in DATA_list :
            pattern = re.compile('[A-Za-z]')
            CID = data['computer_id'][c]
            IANM = []
            if not data['installed_applications_name'][c].startswith('{"[current') and not data['installed_applications_name'][c].startswith('{"TSE-Error') and not data['installed_applications_name'][c].startswith('{"Unknown') and not data['installed_applications_name'][c]== ' '   and not data['installed_applications_name'][c].startswith('[hash'):
                for d in data.installed_applications_name[c].replace('"', '').replace('{', '').replace('}', '').split('!'):
                    if d.startswith(','):
                        d = d.lstrip(',').replace('"','')
                    else :
                        d = d.replace('"','')
                    IANM.append(d)
                IANM.pop(-1)
            else:
                IANM.append('unconfirmed')


            if not data['running_service'][c].startswith('{"[current') and not data['running_service'][c].startswith('{"TSE-Error') and not data['running_service'][c].startswith('Unknown') and not data['running_service'][c] == ' '  and not data['running_service'][c].startswith('[hash'):
                if data['running_service'][c][1] == '"':
                    RS = data['running_service'][c].replace('"','').replace('{','').replace('}','').split(',')
                else:
                    RS = data['running_service'][c].replace('"','').replace('{','').replace('}','').split(',')
            else:
                RS= 'unconfirmed'


            if not data['manufacturer'][c].startswith('[current') and not data['manufacturer'][c].startswith('TSE-Error') and not data['manufacturer'][c].startswith('Unknown') and not data['manufacturer'][c]== ' ' and not data['manufacturer'][c].startswith('[hash'):
                if data['manufacturer'][c] in ['HP', 'HPE']:
                    MF = 'HPE'
                else:
                    MF = data['manufacturer'][c]
            else:
                MF = 'unconfirmed'




            # 세션ip 전처리 추가 예정(server별 session상위5개 ) 새로운 Sensor로 수정한부분
            if not data['session_ip'][c].startswith('{"[current') and "TSE-Error" not in data['session_ip'][c] and not data['session_ip'][c].startswith('{"[Unknown') and not data['session_ip'][c] == ' ':
                if data['session_ip'][c].startswith('{"[no result'):
                    SIP = ['no results']
                elif data['session_ip'][c][2] == '(':
                    SIP = data['session_ip'][c].replace('{', '[').replace('}', ']')
                    SIP = eval(SIP)
                    SIPL = []
                    for a in range(len(SIP)):
                        if not SIP[a].startswith('[hash') and not SIP[a].startswith('Warning') and "TSE-Error" not in SIP[a]:
                            b = eval(SIP[a])
                            SIPL.append(b[1])
                        else:
                            continue
                    SIP = []
                    SIP.append(SIPL)
                else:
                    SIP = data['session_ip'][c].split(',')
                    # SIP = data['session_ip'][c].replace('{', '[').replace('}', ']')
                    # SIP = eval(SIP)
                    SIPL = []
                    for a in range(len(SIP)):
                        if pattern.findall(SIP[a]):
                            continue
                        elif '{' in SIP[a] or '}' in SIP[a]:
                            abc = SIP[a].replace('{', '').replace('}', '')
                            SIPL.append(abc)
                        else:
                            # abc = SIP[a].split(' ')
                            # SIPL.append(abc[1])
                            # print(SIP.split(','))
                            SIPL.append(SIP[a])
                    SIP = []
                    SIP.append(SIPL)
            else:
                SIP = ['unconfirmed']


            #세션ip 전처리 추가 예정(server별 session상위5개 기존 새로운 SessipnIP수정부분 초기)
            # if pattern.findall(data['session_ip'][c]):
            #     SIP = 'NO'
            # if 'current' not in data['session_ip'][c] and 'TSE-Error' not in data['session_ip'][c] and 'Unkown' not in data['session_ip'][c] and not data['session_ip'][c] == ' ':
            #     if data['session_ip'][c].startswith('{"[no result'):
            #         SIP = ['no results']
            #     elif data['session_ip'][c][2] == '(':
            #         SIP = data['session_ip'][c].replace('{', '[').replace('}', ']')
            #         SIP = eval(SIP)
            #         SIPL = []
            #         for a in range(len(SIP)):
            #             if not pattern.findall(SIP[a]):
            #             # if not SIP[a].startswith('[hash') and not SIP[a].startswith('Warning'):
            #                 if '(' in SIP[a]:
            #                     b = eval(SIP[a])
            #                     SIPL.append(b[1])
            #                 else:
            #                     abc = SIP[a].split(' ')
            #                     SIPL.append(abc[1])
            #             else:
            #                 continue
            #         SIP = []
            #         SIP.append(SIPL)
            #
            #     else:
            #         SIP = data['session_ip'][c].replace('{', '[').replace('}', ']')
            #         SIP = eval(SIP)
            #         SIPL = []
            #         for a in range(len(SIP)) :
            #             if '(' in SIP[a]:
            #                 abc = SIP[a].replace('(', '').replace(')', '')
            #                 SIPL.append(eval(abc)[1])
            #             elif pattern.findall(SIP[a]):
            #                 continue
            #             else:
            #                 abc = SIP[a].split(' ')
            #                 SIPL.append(abc[1])
            #         SIP = []
            #         SIP.append(SIPL)
            # else:
            #     SIP = ['unconfirmed']

            if dataType == 'minutely_daily_asset':
                CNM = data['computer_name'][c]
                if data['cup_details_cup_speed'][c] == '' or data['cup_details_cup_speed'][c].startswith('[current') or data['cup_details_cup_speed'][c].startswith('[hash'):
                    CDS = 'unconfirmed'
                else:
                    CDS = data['cup_details_cup_speed'][c].split(' ')[0]
                if not data['last_reboot'][c].startswith('[current') and not data['last_reboot'][c].startswith('TSE-Error') and not data['last_reboot'][c].startswith('Unknown') and not data['last_reboot'][c].startswith('[hash'):
                    LR = datetime.strptime(data['last_reboot'][c].replace('-', '+').split(' +')[0], "%a, %d %b %Y %H:%M:%S")
                else:
                    LR = 'unconfirmed'

                DTS = []
                DTS_item = []
                DTS_sum = 0
                DTS_result = 0
                if not data['disk_total_space'][c].startswith('{"[current') and not data['disk_total_space'][c].startswith('{"TSE-Error') and not data['disk_total_space'][c].startswith('{"Unknown') and not data['disk_total_space'][c].startswith('[hash'):
                    if data['disk_total_space'][c] == None:
                        data['disk_total_space'][c] = 0
                    else:
                        DTS_list = data['disk_total_space'][c].split(',')
                        if len(DTS_list) == 1:
                            item = DTS_list[0].split(' ')
                            DTS_item.append(item)
                        elif len(DTS_list) > 1:
                            for d in DTS_list:
                                item = d.split(' ')
                                DTS_item.append(item)
                        for i in DTS_item:
                            if len(i) == 3:
                                if ('KB' in i[2]):
                                    DTS_result = float(i[1])
                                elif ('MB' in i[2]):
                                    DTS_result = float(i[1]) * 1024
                                elif ('GB' in i[2]):  # 기준
                                    DTS_result = float(i[1]) * 1024 * 1024
                                elif ('TB' in i[2]):
                                    DTS_result = float(i[1]) * 1024 * 1024 * 1024
                                elif ('PB' in i[2]):
                                    DTS_result = float(i[1]) * 1024 * 1024 * 1024 * 1024
                            elif len(i) == 2:
                                if ("K" in i[1].upper()):
                                    item = i[1].upper().find("K")
                                    DTS_result = float(i[1][:item])
                                elif ("M" in i[1].upper()):
                                    item = i[1].upper().find("M")
                                    DTS_result = float(i[1][:item]) * 1024
                                elif ("G" in i[1].upper()):
                                    item = i[1].upper().find("G")
                                    DTS_result = float(i[1][:item]) * 1024 * 1024
                            DTS_sum += DTS_result
                        items = round(DTS_sum / 1024 / 1024)
                    DTS.append(str(items))
                else:
                    DTS.append('unconfirmed')

                DUS = []
                DUS_item = []
                DUS_sum = 0
                DUS_result = 0
                if not data['disk_used_space'][c].startswith('{"[current') and not data['disk_used_space'][c].startswith('{"TSE-Error') and not data['disk_used_space'][c].startswith('{"Unknown') and not data['disk_used_space'][c].startswith('[hash'):
                    if data['disk_used_space'][c] == None:
                        data['disk_used_space'][c] = 0
                    else:
                        DUS_list = data['disk_used_space'][c].split(',')
                        if len(DUS_list) == 1:
                            item = DUS_list[0].split(' ')
                            DUS_item.append(item)
                        elif len(DUS_list) > 1:
                            for d in DUS_list:
                                item = d.split(' ')
                                DUS_item.append(item)
                        for i in DUS_item:
                            if len(i) == 3:
                                if ('KB' in i[2]):
                                    DUS_result = float(i[1])
                                elif ('MB' in i[2]):
                                    DUS_result = float(i[1]) * 1024
                                elif ('GB' in i[2]):  # 기준
                                    DUS_result = float(i[1]) * 1024 * 1024
                                elif ('TB' in i[2]):
                                    DUS_result = float(i[1]) * 1024 * 1024 * 1024
                                elif ('PB' in i[2]):
                                    DUS_result = float(i[1]) * 1024 * 1024 * 1024 * 1024
                            elif len(i) == 2:
                                if ("K" in i[1].upper()):
                                    item = i[1].upper().find("K")
                                    DUS_result = float(i[1][:item])
                                elif ("M" in i[1].upper()):
                                    item = i[1].upper().find("M")
                                    DUS_result = float(i[1][:item]) * 1024
                                elif ("G" in i[1].upper()):
                                    item = i[1].upper().find("G")
                                    DUS_result = float(i[1][:item]) * 1024 * 1024
                            DUS_sum += DUS_result
                        items = round(DUS_sum / 1024 / 1024)
                    DUS.append(str(items).replace('{', '').replace('}', '').replace('[', '').replace(']', ''))
                else:
                    DUS.append('unconfirmed')

                if not data['os_platform'][c].startswith('[current') and not data['os_platform'][c].startswith('TSE-Error') and not data['os_platform'][c].startswith('Unknown')  and not data['os_platform'][c].startswith('[hash'):
                    OP = data['os_platform'][c]
                else:
                    OP = 'unconfirmed'
                if not data['operating_system'][c].startswith('[current') and not data['operating_system'][c].startswith('TSE-Error') and not data['operating_system'][c].startswith('Unknown') and not data['operating_system'][c].startswith('[hash'):
                    OS = re.sub(r'\(core\)|\(Final\)', '', data['operating_system'][c])
                    OS = re.sub(r'(\.\d+)*', '', OS).strip()
                    #OS = data['operating_system'][c]
                else:
                    OS = 'unconfirmed'
                if not data['is_virtual'][c].startswith('[current') and not data['is_virtual'][c].startswith('TSE-Error') and not data['is_virtual'][c].startswith('Unknown') and not data['is_virtual'][c].startswith('[hash'):
                    IV = data['is_virtual'][c]
                else:
                    IV = 'unconfirmed'
                if not data['chassis_type'][c].startswith('[current') and not data['chassis_type'][c].startswith('TSE-Error') and not data['chassis_type'][c].startswith('Unknown') and not data['chassis_type'][c].startswith('[hash'):
                    CT = data['chassis_type'][c]
                else:
                    CT = 'unconfirmed'
                if not data['ipv_address'][c].startswith('[current') and not data['ipv_address'][c].startswith('TSE-Error') and not data['ipv_address'][c].startswith('Unknown') and not data['ipv_address'][c].startswith('[hash'):
                    IPV = data['ipv_address'][c]
                else:
                    IPV = 'unconfirmed'

                if not data['today_listen_port_count'][c] == None and not data['today_listen_port_count'][c].startswith('[current') and not data['today_listen_port_count'][c].startswith('TSE-Error') and not data['today_listen_port_count'][c].startswith('Unknown') and not data['today_listen_port_count'][c].startswith('[hash'):
                    LPC = data['today_listen_port_count'][c]
                else:
                    LPC = 'unconfirmed'

                if not data['today_established_port_count'][c] == None and not data['today_established_port_count'][c].startswith('[current') and not data['today_established_port_count'][c].startswith('TSE-Error') and not data['today_established_port_count'][c].startswith('Unknown') and not data['today_established_port_count'][c].startswith('[hash'):
                    EPC = data['today_established_port_count'][c]
                else:
                    EPC = 'unconfirmed'

                if not data['yesterday_listen_port_count'][c] == None and not data['yesterday_listen_port_count'][c].startswith('[current') and not data['yesterday_listen_port_count'][c].startswith('TSE-Error') and not data['yesterday_listen_port_count'][c].startswith('Unknown') and not data['yesterday_listen_port_count'][c].startswith('[hash'):
                    YLPC = data['yesterday_listen_port_count'][c]
                else:
                    YLPC = 'unconfirmed'

                if not data['yesterday_established_port_count'][c] == None and not data['yesterday_established_port_count'][c].startswith('[current') and not data['yesterday_established_port_count'][c].startswith('TSE-Error') and not data['yesterday_established_port_count'][c].startswith('Unknown') and not data['yesterday_established_port_count'][c].startswith('[hash'):
                    YEPC = data['yesterday_established_port_count'][c]
                else:
                    YEPC = 'unconfirmed'

                if not data['ram_use_size'][c].startswith('[current') and not data['ram_use_size'][c].startswith('TSE-Error') and not data['ram_use_size'][c].startswith('Unknown') and not data['ram_use_size'][c].startswith('[hash'):
                    RUS = data['ram_use_size'][c].split(' ')[0]
                else:
                    RUS = 'unconfirmed'

                if not data['ram_total_size'][c].startswith('[current') and not data['ram_total_size'][c].startswith('TSE-Error') and not data['ram_total_size'][c].startswith('Unknown') and not data['ram_total_size'][c].startswith('[hash'):
                    RTS = data['ram_total_size'][c].split(' ')[0]
                else:
                    RTS = 'unconfirmed'

                if not data['cup_consumption'][c].startswith('[current') and not data['cup_consumption'][c].startswith('TSE-Error') and not data['cup_consumption'][c].startswith('[hash'):
                    CPUC = float(data['cup_consumption'][c].split(' ')[0])
                else:
                    CPUC = data['cup_consumption'][c]

                if not data['online'][c].startswith('[current') and not data['online'][c].startswith(
                        'TSE-Error') and not data['online'][c].startswith('Unknown') and not data['online'][c].startswith('[hash'):
                    OL = data['online'][c]
                else:
                    OL = 'unconfirmed'


                if not data['tanium_client_subnet'][c].startswith('[current') and not data['tanium_client_subnet'][c].startswith(
                        'TSE-Error') and not data['tanium_client_subnet'][c].startswith('Unknown') and not data['tanium_client_subnet'][c].startswith('Can not determine') and not data['tanium_client_subnet'][c].startswith('[hash'):
                    if '0.0.0.0' in data['tanium_client_subnet'][c]:
                        TCS = 'unconfirmed'
                    elif data['tanium_client_subnet'][c] == '192.168.0.0/20' or data['tanium_client_subnet'][c] == '172.18.112.0/21':
                        TCS = '게임서버팜'
                    elif data['tanium_client_subnet'][c] == '172.20.0.0/21' or data['tanium_client_subnet'][c] == '172.20.160.0/21' or data['tanium_client_subnet'][c] == '172.20.0.0/22':
                        TCS = '인프라서버팜'
                    elif data['tanium_client_subnet'][c] == '172.20.92.0/22' or data['tanium_client_subnet'][c] == '172.20.96.0/22':
                        TCS = '플랫폼서버팜'
                    elif data['tanium_client_subnet'][c] == '172.20.80.0/21':
                        TCS = '구 인터넷 개발망'
                    elif data['tanium_client_subnet'][c] == '172.19.72.0/21' or data['tanium_client_subnet'][c] == '172.19.74.0/22':
                        TCS = 'Qtest망'
                    elif data['tanium_client_subnet'][c] == '211.189.164.0/24':
                        TCS = 'DMZ망'
                    elif data['tanium_client_subnet'][c] == '170.0.0.0/8':
                        TCS = '10LanPC망'
                    elif data['tanium_client_subnet'][c] == '172.19.64.0/22' or data['tanium_client_subnet'][c] == '172.19.64.0/21':
                        TCS = 'Sandbox망'
                    else:
                        TCS = '기타'
                else:
                    TCS = 'unconfirmed'



                NS= []
                if data['nvidia_smi'][c].startswith('{"[current') or data['nvidia_smi'][c].startswith(
                        '{"TSE-Error') or data['nvidia_smi'][c].startswith('{"[Unknown') or data['nvidia_smi'][c] == ' ' or data['nvidia_smi'][c].startswith('[hash'):
                    NS = 'unconfirmed'
                elif data['nvidia_smi'][c].startswith('{"[no result'):
                    NS = 'no results'
                else:
                    NS_item = []
                    NS_count = 0
                    NS_list = data['nvidia_smi'][c].split(',')
                    NS_count = NS_list[0].split(': ')[1].replace('"', '')
                    NS_item = NS_list[1].split(': ')[1].replace('"}', '')

                    NS = [NS_count, str(NS_item)]

                if not data['wire'][c].startswith('[current') and not data['wire'][c].startswith('TSE-Error') and not data['wire'][c].startswith('Unknown') and not data['wire'][c].startswith('[hash') and not data['wire'][c].startswith('[no result'):
                    WIRE = data['wire'][c].split(' ')[0]
                else:
                    WIRE = 'unconfirmed'

                DL.append([CID, CNM, LR, DTS, DUS, OP, OS, IV, CT, IPV, LPC, YLPC, EPC, YEPC, RUS, RTS, IANM, RS, CPUC, OL, TCS, MF, SIP, NS, CDS, WIRE])
            elif dataType == 'minutely_asset':
                DL.append([CID, IANM, MF, RS, SIP])
        logger.info('Preprocessing.py - ' + dataType + ' 성공')
        return DL
    except Exception as e:
        logger.warning('Preprocessing_OM.py - Error 발생')
        logger.warning('Error : ' + str(e))



