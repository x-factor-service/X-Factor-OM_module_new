import pandas as pd

def plug_in(data) :
    DFL = []
    resources = ['cpu', 'mem', 'disk']
    for d in data:
        for idx, rsc in enumerate(resources):
            if 'result' not in d[4 + idx][0]['text'] and 'Error' not in d[4 + idx][0]['text']:
                for item in d[4 + idx]:
                    CI = d[0][0]['text']
                    CN = d[1][0]['text']
                    OS = d[2][0]['text']
                    IP = d[3][0]['text']
                    TN = item['text']
                    DFL.append([CI, CN, OS, IP, TN, rsc])
    DFC = ['computer_id', 'computer_name', 'os', 'ip', 'task_name', 'resource']
    DF = pd.DataFrame(DFL, columns=DFC)
    return DF
