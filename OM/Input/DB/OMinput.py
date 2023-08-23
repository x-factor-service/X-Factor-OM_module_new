import math

import psycopg2
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd

with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())
DataLoadingType = SETTING['MODULE']['DataLoadingType']
DBHost = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['HOST']
DBPort = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['PORT']
DBName = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['NAME']
DBUser = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['USER']
DBPwd = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['PWD']
AssetTNM = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['TNM']['DA']
StatisticsTNM = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['TNM']['DS']
BS = SETTING['FILE']
DBSettingTime = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['DBSelectTime']
day = datetime.today().strftime("%Y-%m-%d")
RSU = SETTING['FILE']['RunningService_Except']['USE']


def plug_in(table, day, type):
    try:
        FiveMinuteAgo = (datetime.today() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
        DBSelectTime = (datetime.today() - timedelta(minutes=DBSettingTime)).strftime("%Y-%m-%d %H:%M:%S")
        halfHourAgo = (datetime.today() - timedelta(minutes=35)).strftime("%Y-%m-%d %H:%M:%S")
        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        fiveDay = (datetime.today() - timedelta(5)).strftime("%Y-%m-%d")
        # monthDay = (datetime.today() - timedelta(30)).strftime("%Y-%m-%d")
        monthDay = (datetime.today() - relativedelta(days=31)).strftime("%Y-%m-%d")
        weekDay = (datetime.today() - timedelta(7)).strftime("%Y-%m-%d")
        # ----------------------서버수량그래프 데이터 변경 추가 종윤 ----------------------
        lastYear = (datetime.today() - relativedelta(months=12)).strftime("%Y-%m-%d")
        lastDay = (datetime.today() - relativedelta(months=11)).strftime("%Y-%m-%d")
        lastMonth = pd.date_range(lastDay, periods=12, freq='M').strftime("%Y-%m-%d")
        Reportday2 = (datetime.today() - timedelta(2)).strftime("%Y-%m-%d")
        Reportday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")

        a = []
        for i in lastMonth:
            a.append(str(i))
        LM = tuple(a)

        # ------------------------------------------------------------------------------
        month_str = (datetime.today() - relativedelta(months=1)).strftime("%Y-%m-%d")
        SDL = []
        Conn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHost, DBPort, DBName, DBUser, DBPwd))
        Cur = Conn.cursor()
        if table == 'asset':
            if day == 'yesterday':
                query = """
                    select 
                        computer_id, disk_used_space, listen_port_count, established_port_count, asset_collection_date
                    from
                        """ + AssetTNM + """
                    where 
                        to_char(asset_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """'
                    order by computer_id desc
                """

        if table == 'statistics':
            if day == 'yesterday':
                if type == '':
                    query = """ 
                        select 
                            classification, item, item_count, statistics_collection_date
                        from 
                            daily_statistics
                        where 
                            to_char(statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """'
                        and 
                            NOT classification IN ('installed_applications')
                        and
                            NOT classification IN ('running_service')
                        and
                            NOT classification IN ('session_ip')
                        and 
                            classification NOT like '%group_%'
                        and
                            NOT item IN ('unconfirmed')
                    """

                if type == 'bannerNC':
                    query = """
                        select 
                            classification, item, item_count, statistics_collection_date
                        from
                            daily_statistics
                        where 
                            classification in ('online_asset', 'virtual', 'os', 'group_server_count')
                            and NOT item IN ('unconfirmed')
                            and to_char(statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """'                  
                    """

            if day == 'today':
                if type == '':
                    query = """ 
                        select 
                            classification, item, item_count, statistics_collection_date
                        from 
                            minutely_statistics
                        where 
                            NOT classification IN ('installed_applications')
                        and
                            NOT classification IN ('running_processes')
                        and
                            NOT classification IN ('session_ip')
                        and 
                            classification NOT like '%group_%'
                        and
                            NOT item IN ('unconfirmed')
                        and 
                            statistics_collection_date >= '""" + DBSelectTime + """'
                    """

                elif type == 'bar':
                    query = """
                        select 
                            item, item_count 
                        from 
                            minutely_statistics  
                        where 
                            classification ='asset'
                        and 
                            statistics_collection_date >= '""" + DBSelectTime + """'
                        order by 
                            item_count desc limit 3    
                    """
                elif type == 'pie':
                    query = """
                        select item, item_count from 
                        minutely_statistics 
                        where 
                            classification = 'os'
                        and 
                            statistics_collection_date >= '""" + DBSelectTime + """' 
                        order by item_count::INTEGER desc limit 3
                    """
                elif type == 'os_version':
                    query = """
                        select 
                            item, item_count
                        from 
                            minutely_statistics
                        where 
                            classification = 'operating_system' 
                        and 
                            statistics_collection_date >= '""" + DBSelectTime + """'
                        order by item_count::INTEGER desc limit 8
                    """
                elif type == 'donut':
                    query = """
                        select 
                            item, item_count 
                        from 
                            minutely_statistics
                        where
                            classification = 'installed_applications'
                        and 
                            statistics_collection_date >= '""" + DBSelectTime + """'
                        order by
                            item_count::INTEGER 
                        desc limit 5
                    """

                elif type == 'case':
                    query = """
                        select
                            computer_id, ipv_address, driveusage
                        from
                            minutely_statistics_list
                    """

                # NC 대역별 barchart
                elif type == 'group_server_count':
                    query = """
                            select 
                                item, item_count 
                            from 
                                minutely_statistics  
                            where 
                                classification ='group_server_count'
                            and 
                                statistics_collection_date >= '""" + DBSelectTime + """'
                            order by
                                item_count::INTEGER 
                            desc limit 3
                        """
                # NC running service chart
                elif type == 'running':
                    # 러닝서비스 프로그램 지우기
                    try:
                        runningservice_locate = SETTING['FILE']['RunningService_Except']['Location']
                        readXls = pd.read_excel(runningservice_locate, na_values='None')
                        running_remove = []
                        for i in readXls.index:
                            running_remove.append(readXls['Running Service'][i])
                        running_tu = tuple(running_remove)
                        running_remove = str(running_tu)
                        query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where 
                                classification = 'running_service'
                            and
                                item NOT IN """ + running_remove + """
                            and 
                                statistics_collection_date >= '""" + DBSelectTime + """'

                            order by
                                item_count::INTEGER desc limit 5
                        """
                    except:
                        query = """
                                select
                                    item, item_count
                                from
                                    minutely_statistics
                                where 
                                    classification = 'running_service'
                                and 
                                    statistics_collection_date >= '""" + DBSelectTime + """'

                                order by
                                    item_count::INTEGER desc limit 5
                            """
                # 알람케이스
                elif type == 'usage':
                    query = """
                        select
                            classification, item, item_count
                        from
                            minutely_statistics
                        where 
                            classification in ('ram_usage_size_exceeded', 'cpu_usage_size_exceeded', 'drive_usage_size_exceeded', 'last_online_time_exceeded')
                        and
                            NOT item IN ('unconfirmed', 'No', 'Safety')
                        and 
                            statistics_collection_date >= '""" + FiveMinuteAgo + """'

                    """


                elif type == 'cpuNormal':
                    query = """
                        select
                            ms.classification, ms.item, ms.item_count
                        from
                            minutely_statistics ms 
                        where
                            classification = 'cpu_usage_size_exceeded'
                        and
                            statistics_collection_date >= '""" + DBSelectTime + """'
                    """
                elif type == 'memoryNormal':
                    query = """
                            select
                                ms.classification, ms.item, ms.item_count
                            from
                                minutely_statistics ms 
                            where
                                classification = 'ram_usage_size_exceeded'
                            and
                                statistics_collection_date >= '""" + DBSelectTime + """'
                        """
                elif type == 'diskNormal':
                    query = """
                            select
                                ms.classification, ms.item, ms.item_count
                            from
                                minutely_statistics ms 
                            where
                                classification = 'drive_usage_size_exceeded'
                            and
                                statistics_collection_date >= '""" + DBSelectTime + """'
                        """

                    # ----------------------Main dashboard 디스크 메모리 도넛차트용 데이터-------------------------------
                elif type == 'ResourceRamUsage':
                    query = """
                        select
                            classification, item, item_count
                        from
                            minutely_statistics
                        where
                            classification in ('ram_usage_size_exceeded')
                        and
                            item in ('95Risk')
                        and
                            NOT item IN ('unconfirmed', 'No', 'Safety')
                        and
                            statistics_collection_date >= '""" + DBSelectTime + """'

                    """
                elif type == 'ResourceDiskUsage':
                    query = """
                            select
                                classification, item, item_count
                            from
                                minutely_statistics
                            where
                                classification in ('drive_usage_size_exceeded')
                            and
                                item in ('95Risk')
                            and
                                NOT item IN ('unconfirmed', 'No', 'Safety')
                            and
                                statistics_collection_date >= '""" + DBSelectTime + """'

                        """

                # 물리서버 벤더별 수량
                elif type == 'vendor':
                    query = """
                        select
                            item, item_count
                        from
                            minutely_statistics
                        where
                            classification = 'manufacturer'
                        and 
                            statistics_collection_date >= '""" + DBSelectTime + """'
                        order by
                            item_count::INTEGER desc 
                        limit 3
                    """
                # IP 대역별 총 알람 수(Top5)
                elif type == 'group_alarm':
                    query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where
                                classification IN
                                ('group_ram_usage_exceeded',
                                'group_last_online_time_exceeded',
                                'group_cpu_usage_exceeded',
                                'group_drive_usage_size_exceeded')
                                AND item != 'unconfirmed'
                                and statistics_collection_date >= '""" + FiveMinuteAgo + """'
                            """
                elif type == 'bannerNC':
                    query = """
                            select 
                                classification, item, item_count, statistics_collection_date
                            from
                                minutely_statistics
                            where 
                                classification in ('online_asset', 'virtual', 'os', 'group_server_count')
                                and NOT item IN ('unconfirmed')
                                and statistics_collection_date >= '""" + DBSelectTime + """'
                            """
                elif type == 'gpu':
                    query = """
                            select
                                item, item_count
                            from
                                minutely_statistics
                            where
                                classification = 'nvidia_smi'
                            and
                                item = 'YES'
                            and 
                                statistics_collection_date >= '""" + DBSelectTime + """'
                            union all
                            select
                                item, item_count
                            from
                                daily_statistics
                            where
                                classification = 'nvidia_smi'
                            and
                                item = 'YES'
                            and 
                                to_char(statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """'
                    """

                elif type == 'idle':
                    query = """
                                select 
                                    TO_CHAR(statistics_collection_date, 'YYYY-MM-DD') ,item_count
                                from
                                    daily_statistics
                                where 
                                    item = 'collection_date'
                                    and statistics_collection_date >= '""" + yesterday + """'
                                order by statistics_collection_date asc
                            """

                elif type == 'ip':
                    query = """
                            select
                                minutely_statistics_unique, classification, item, item_count
                            from
                                minutely_statistics
                            where
                                classification = 'session_ip_computer_name'
                            and
                                statistics_collection_date >= '""" + DBSelectTime + """'
                            order by
                                item_count::INTEGER desc 
                            limit 3
                    """


                elif type == 'os':
                    query = """
                            select 
                                item, item_count
                            from
                                minutely_statistics
                            where 
                                classification = 'os'
                                AND item != 'unconfirmed'
                                and NOT item IN ('unconfirmed')
                                and statistics_collection_date >= '""" + DBSelectTime + """'
                            order by item_count desc
                    """

                elif type == 'virtual':
                    query = """
                            select 
                                item, item_count
                            from
                                minutely_statistics
                            where 
                                classification = 'virtual'
                                AND item != 'unconfirmed'
                                and NOT item IN ('unconfirmed')
                                and statistics_collection_date >= '""" + DBSelectTime + """'
                            order by item desc
                    """
            # NC 서버 총 수량 추이 그래프(30일)
            if day == 'monthly':
                if type == 'asset':
                    query = """
                                select
                                    item,
                                    item_count,
                                    statistics_collection_date
                                from
                                    daily_statistics
                                where
                                    to_char(statistics_collection_date, 'YYYY-MM-DD')
                                in 
                                    """ + str(LM) + """
                                and
                                    classification = 'virtual'
                                and
                                    item != 'unconfirmed'
                                union
                                select
                                    item,
                                    item_count,
                                    statistics_collection_date
                                from
                                    minutely_statistics
                                where
                                    classification = 'virtual'
                                and
                                    item != 'unconfirmed'
                                order by
                                    statistics_collection_date ASC;
                            """

            if day == 'fiveDay':
                if type == 'asset':
                    query = """ 
                        select 
                            classification,
                            item, 
                            item_count, 
                            statistics_collection_date
                        from 
                            daily_statistics 
                        where 
                            to_char(statistics_collection_date, 'YYYY-MM-DD') > '""" + fiveDay + """' 
                        and
                            classification = '""" + type + """'
                        order by
                            item_count desc;
                    """
            if day == 'assetItem':
                if type == 'Group':
                    query = """
                        select 
                            item, 
                            item_count  
                        from 
                            minutely_statistics 
                        where 
                            classification ='asset' 
                        and 
                            statistics_collection_date >= '""" + DBSelectTime + """'
                    """
            if type == 'ram':
                query = """
                    select
                        classification, item, item_count
                    from
                        minutely_statistics
                    where 
                        classification in ('group_ram_usage_exceeded')
                    and 
                            statistics_collection_date >= '""" + DBSelectTime + """'
                    order by
                        item_count::INTEGER desc limit 5
                """
            if type == 'cpu':
                query = """
                    select
                        classification, item, item_count
                    from
                        minutely_statistics
                    where 
                        classification in ('group_cpu_usage_exceeded')
                    and 
                        statistics_collection_date >= '""" + DBSelectTime + """'
                    order by
                        item_count::INTEGER desc limit 5
                """
            if type == 'world':
                query = """
                    select
                        classification, item
                    from
                        minutely_statistics
                    where 
                        classification in ('group_cpu_usage_exceeded', 'group_ram_usage_exceeded', 'group_running_service_count_exceeded', 
                        'group_last_reboot', 'drive_usage_size_exceeded')
                    and 
                        statistics_collection_date >= '""" + DBSelectTime + """'
                """
        if table == 'statistics_list':
            if day == 'today':
                if type == 'DUS':
                    query = """
                            select
                                computer_id, driveusage, ipv_address
                            from
                                minutely_statistics_list
                        """
                elif type == 'statistics':
                    query = """
                        select
                            classification, item, item_count
                        from
                            minutely_statistics
                        where 
                            classification IN ('established_port_count_change', 
                                'group_running_service_count_exceeded',
                                'group_cpu_usage_exceeded',
                                'group_ram_usage_exceeded',
                                'listen_port_count_change',
                                'group_last_reboot',
                                'drive_usage_size_exceeded')
                            and 
                                NOT item IN ('unconfirmed')

                    """
                elif type == 'LH':
                    query = """
                        select
                            computer_id, last_reboot, ipv_address
                        from
                            minutely_statistics_list
                    """
                elif type == 'RUS':
                    query = """
                        select
                            computer_id, ramusage, ipv_address
                        from
                            minutely_statistics_list
                    """
                elif type == 'server':
                    query = """
                        select
                            ipv_address, computer_name, session_ip_count
                        from
                            minutely_statistics_list
                        where
                            asset_list_statistics_collection_date >= '""" + DBSelectTime + """'
                            and NOT ipv_address IN ('unconfirmed')
                        order by
                            session_ip_count::INTEGER desc limit 3
                    """
                elif type == 'memoryMore':
                    query = """
                        select
                            ipv_address, computer_name, ram_use_size, ram_total_size, ramusage
                        from
                            minutely_statistics_list
                        order by
                            ramusage desc
                    """
            if day == 'yesterday':
                if type == 'DUS':
                    query = """
                        select
                            computer_id, driveusage, ipv_address
                        from
                            daily_statistics_list
                        where 
                            to_char(asset_list_statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """' 
                    """
                elif type == 'LH':
                    query = """
                        select
                            computer_id, last_reboot, ipv_address
                        from
                            daily_statistics_list
                        where 
                            to_char(asset_list_statistics_collection_date, 'YYYY-MM-DD') = '""" + yesterday + """' 
                    """
        Cur.execute(query)
        RS = Cur.fetchall()
        for i, R in enumerate(RS, start=1):
            if type in ['idle']:
                SDL.append(dict(
                    (
                        ('item', R[0]),
                        ('count', int(R[1]))
                    )
                ))
            else:
                SDL.append(R)
        return SDL
    except:
        print(table + str(type) + day + ' Daily Table connection(Select) Failure')
    return