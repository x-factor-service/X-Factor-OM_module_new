import urllib3
import json
from common.core.transform import  transform_pieData, transform_donutData
from common.output.db import plug_in as outputDb
from common.module.Input.DiscoverInput import plug_in as disInput
from common.module.Input.HighCpuProcInput import plug_in as highCpuProcInput
from common.module.Output.DiscoverOutput import plug_in as disOut
from common.module.Output.idleAssetOutput import plug_in as IdleOut
from common.module.Transform.IdleAssetDataframe import plug_in as IdleDF
from common.module.Transform.SbomDataframe import plug_in as SbomDF
from common.module.Transform.HighCpuProcTransform import plug_in as highCpuProc_transform
from common.module.Output.SBOMOutput import plug_in as SbomOut
from common.module.Input.idleAssetInput import plug_in_DB
from common.module.Transform.CertificateDataframe import plug_in as CrtDF
from common.module.Output.CertificateOutput import plug_in as CrtOut
from common.module.Output.HighCpuProcOutput import plug_in as highCpuProcOutput
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())
SOMIPAPIU = SETTING['CORE']['Tanium']['SOURCE']['MINUTELY']['INPUT']['API'].lower()  # (Source Data MINUTELY input plug in API 사용 여부 설정)
SOMTPIU = SETTING['CORE']['Tanium']['SOURCE']['MINUTELY']['Transform'].lower()  # (Source Data MINUTELY Transform(preprocessing) plug in 사용 여부 설정)
SOMOPIDBPU = SETTING['CORE']['Tanium']['SOURCE']['MINUTELY']['OUTPUT']['DB']['PS'].lower()  # (Source Data MINUTELY Output plug in postgresql DB 사용 여부 설정)
STCU = SETTING['CORE']['Tanium']['STATISTICS']['COLLECTIONUSE'].lower()  # (통계 Data 수집 여부 설정)
STMIPIDBPU = SETTING['CORE']['Tanium']['STATISTICS']['MINUTELY']['INPUT']['DB']['PS'].lower()  # (통계 Data MINUTELY input plug in postgresql DB 사용 여부 설정)
STMTPIU = SETTING['CORE']['Tanium']['STATISTICS']['MINUTELY']['Transform'].lower()  # (통계 Data MINUTELY Transform(preprocessing) plug in 사용 여부 설정)
STMOPODBPU = SETTING['CORE']['Tanium']['STATISTICS']['MINUTELY']['OUTPUT']['DB']['PS'].lower()  # (통계 Data MINUTELY Output plug in postgresql DB 사용 여부 설정)



def minutely_plug_in():
    # ------------------------------상단 디스크 사용률 도넛 차트------------------------
    disk_donutData = transform_donutData('Disk Used Percentage#3')
    outputDb(disk_donutData, 'minutely')
    # -----------------------------상단 메모리 사용률 도넛 차트------------------------------
    memory_donutData = transform_donutData('Memory Consumption#2')
    outputDb(memory_donutData, 'minutely')
    # -----------------------------상단 씨피유 사용률 도넛 차트------------------------------
    cpu_donutData = transform_donutData('CPU Consumption#2')
    outputDb(cpu_donutData, 'minutely')
    # -----------------------------상단 오에스 파이차트 ------------------------------------
    os_pieData = transform_pieData('OS Platform')
    outputDb(os_pieData, 'minutely')
    # -----------------------------상단 유/무선(와이어) 파이차트 ------------------------------------
    wire_pieData = transform_pieData('wired/wireless 2')
    outputDb(wire_pieData, 'minutely')
    # -----------------------------상단 물리/가상 파이차트 ------------------------------------
    virtual_pieData = transform_pieData('Is Virtual#3')
    outputDb(virtual_pieData, 'minutely')

    # ------------------------------------- SBOM -----------------------------------------
    sbomOutputData = SbomDF()
    SbomOut(sbomOutputData, 'sbom_list')

    # ------------------------------------- 하단 최대 CPU 프로세스 --------------------------
    highCpuProcessInputData = highCpuProcInput()
    highCpuProcDF = highCpuProc_transform(highCpuProcessInputData)
    highCpuProcOutput(highCpuProcDF)

    try:
        from CSPM.CORE.Dashboard import minutely_plug_in as CSPM_minutzely_plug_in
        CSPM_minutely_plug_in()
    except (ImportError, NameError):
        pass
    try:
        from OM.CORE.Dashboard import minutely_plug_in as OM_minutely_plug_in
        OM_minutely_plug_in()
    except (ImportError, NameError):
        pass
    try:
        from SM.CORE.Dashboard import minutely_plug_in as SM_minutely_plug_in
        SM_minutely_plug_in()
    except (ImportError, NameError) :
        pass


def daily_plug_in():
    # -----------------------------인증서 목록  ------------------------------------
    certificate_Data = CrtDF()
    CrtOut(certificate_Data, 'list')
    CrtOut(certificate_Data, 'statistics')


    try:
        from CSPM.CORE.Dashboard import daily_plug_in as CSPM_daily_plug_in
        CSPM_daily_plug_in()
    except (ImportError, NameError):
        pass
    try:
        from OM.CORE.Dashboard import daily_plug_in as OM_daily_plug_in
        OM_daily_plug_in()
    except (ImportError, NameError):
        pass
    try:
        from SM.CORE.Dashboard import daily_plug_in as SM_daily_plug_in
        SM_daily_plug_in()
    except (ImportError, NameError) :
        pass
    # -----------------------------예상/유휴자산 ------------------------------------
    idleOutputData=IdleDF()
    IdleOut(idleOutputData, 'asset')
    idleInputData = plug_in_DB()
    IdleOut(idleInputData, 'statistics')

    # -----------------------------중앙 관리/미관리 라인차트 ----------------------------------
    disOriginData = disInput()
    disOut(disOriginData)