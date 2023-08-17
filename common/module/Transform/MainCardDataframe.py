import pandas as pd

def main_cardDF(data):
    DFL = []
    DFC = ['computer_id', 'computer_name', 'disk_gb3', 'disk_used_space', 'disk_free_space', 'used_memory', 'total_memory', 'os_platform', 'wired', 'is_virtual', 'cpu_consumption']
    for d in data:
        CI = d[0][0]['text']
        CN = d[1][0]['text']
        DG = d[2][0]['text']
        DUS = d[3][0]['text']
        DFS = d[4][0]['text']
        UM = d[5][0]['text']
        TM = d[6][0]['text']
        OP = d[7][0]['text']
        WID = d[8][0]['text']
        IVI = d[9][0]['text']
        CPUN = d[10][0]['text']
        DFL.append([CI, CN, DG, DUS, DFS, UM, TM, OP, WID, IVI, CPUN])
    DF = pd.DataFrame(DFL, columns=DFC)
    return DF


def parse_size(size_str):
    try:
        if size_str == "[current result unavailable]" or size_str == "[results":
            return 0.0
        size = float(size_str.split()[0])
    except Exception as e:
        size = None
    return size

def disk_usage(DF):
    disk_df = DF[['disk_gb3', 'disk_used_space']].copy()
    disk_df['disk_gb3'] = disk_df['disk_gb3'].apply(parse_size)
    disk_df['disk_used_space'] = disk_df['disk_used_space'].apply(parse_size)
    # drop the rows where either 'disk_gb3' or 'disk_used_space' is None
    disk_df.dropna(subset=['disk_gb3', 'disk_used_space'], inplace=True)
    disk_df['usage_rate'] = disk_df['disk_used_space'] / disk_df['disk_gb3'] * 100
    DU = (disk_df['usage_rate'] >= 95).sum()
    return DU


def parse_memory(memory_str):
    try:
        if memory_str == "[current result unavailable]" or memory_str == "[results":
            return 0.0
        memory = float(memory_str.split()[0])
    except Exception as e:
        memory = None
    return memory

def memory_usage(DF):
    memory_df = DF[['used_memory', 'total_memory']].copy()
    memory_df['used_memory'] = memory_df['used_memory'].apply(parse_memory)
    memory_df['total_memory'] = memory_df['total_memory'].apply(parse_memory)
    # drop the rows where either 'used_memory' or 'total_memory' is None
    memory_df.dropna(subset=['used_memory', 'total_memory'], inplace=True)
    memory_df['usage_rate'] = memory_df['used_memory'] / memory_df['total_memory'] * 100
    MU = (memory_df['usage_rate'] >= 95).sum()
    return MU


def os_counts(DF):
    DF = DF[DF['os_platform'] != '[current result unavailable]']
    os_counts_df = DF['os_platform'].value_counts().reset_index()
    os_counts_df.columns = ['os_platform', 'count']
    return os_counts_df

def wired_counts(DF):
    mask = ~DF['wired'].str.contains('result|Error', case=False, na=False)
    DF_filtered = DF[mask].copy()
    wired_counts = DF_filtered['wired'].value_counts().reset_index()
    wired_counts.columns = ['wired', 'count']
    return wired_counts


def virtual_counts(DF):
    DF = DF[DF['is_virtual'] != '[current result unavailable]']
    virtual_counts_df = DF['is_virtual'].value_counts().reset_index()
    virtual_counts_df.columns = ['is_virtual', 'count']
    return virtual_counts_df

def cpu_usage(DF):
    mask = ~DF['cpu_consumption'].str.contains('result|Error', case=False, na=False)
    DF_filtered = DF[mask].copy()
    DF_filtered.loc[:, 'cpu_percentage'] = DF_filtered['cpu_consumption'].str.replace('%', '').astype(float)
    cpu_usage_count = DF_filtered[DF_filtered['cpu_percentage'] >= 95].shape[0]
    return cpu_usage_count

def plug_in(data):
    DF = main_cardDF(data)
    DU = disk_usage(DF)
    MU = memory_usage(DF)
    ON = os_counts(DF)
    WC = wired_counts(DF)
    VI = virtual_counts(DF)
    CPU_USAGE = cpu_usage(DF)
    return DF, DU, MU, ON, WC, VI, CPU_USAGE

def daily_os(data):
    counts = {}
    for item in data:
        count = counts.get(item[0], 0)
        counts[item[0]] = count + 1
    result = pd.DataFrame(list(counts.items()), columns=['Value', 'Count'])
    return result