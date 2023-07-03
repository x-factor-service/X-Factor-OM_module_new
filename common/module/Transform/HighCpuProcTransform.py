import pandas as pd

def plug_in(data) :
    DFL = []
    DFC = ['computer_id', 'computer_name', 'os', 'ip', 'proc_name']
    #print(data)
    for d in data:
        if d[4][0]['text'] == '[no results]' or d[4][0]['text'] == '[current result unavailable]':
            pass
        else:
            for i in range(len(d[4])):
                CI = d[0][0]['text']
                CN = d[1][0]['text']
                OS = d[2][0]['text']
                IP = d[3][0]['text']
                PRCN = d[4][i]['text']
                DFL.append([CI, CN, OS, IP, PRCN])
        DF = pd.DataFrame(DFL, columns=DFC)
    # elif type == 'CNT' :
    #     proc_counts = data['proc_name'].value_counts().reset_index()
    #     proc_counts.columns = ['proc_name', 'cnt']
    #     DF = proc_counts
    return DF

