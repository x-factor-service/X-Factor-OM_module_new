import pandas as pd
from common.module.Input.CertificateInput import plug_in as certificate
from common.input.Session import plug_in as session


def plug_in() :
    SK = session()
    data = certificate(SK)
    DFL = []
    DFC = [
        'computer_id', 'computer_name', 'os', 'ip', 'crt_name', 'crt_expire_date']
    for d in data:
        if d[4][0]['text'] == '[no results]' or d[4][0]['text'] == '[current result unavailable]' :
            pass
        else:
            for i in range(len(d[4])):
                CI = d[0][0]['text']
                CN = d[1][0]['text']
                OS = d[2][0]['text']
                IP = d[3][0]['text']
                CRTN = d[4][i]['text']
                CRTED = d[5][i]['text']
                DFL.append([CI, CN, OS, IP, CRTN, CRTED])
        DF = pd.DataFrame(DFL, columns=DFC)
    return DF