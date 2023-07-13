import urllib3
import json

from common.module.Input.DiscoverInput import plug_in as disInput
from common.module.Input.HighCpuProcInput import plug_in as highCpuProcInput
from common.module.Input.ReportInput import plug_in as reportInput
from common.module.Output.DiscoverOutput import plug_in as disOut
from common.module.Output.idleAssetOutput import plug_in as IdleOut
from common.module.Output.ReportOutput import plug_in as reportOut
from common.module.Transform.IdleAssetDataframe import plug_in as IdleDF
from common.module.Transform.SbomDataframe import plug_in as SbomDF
from common.module.Transform.HighCpuProcTransform import plug_in as highCpuProc_transform
from common.module.Transform.ReportTransform import plug_in as report_transform
from common.module.Output.SBOMOutput import plug_in as SbomOut
from common.module.Input.idleAssetInput import plug_in_DB
from common.module.Transform.CertificateDataframe import plug_in as CrtDF
from common.module.Output.CertificateOutput import plug_in as CrtOut
from common.module.Output.HighCpuProcOutput import plug_in as highCpuProcOutput
from common.module.Input.MainCardInput import plug_in as mcInput, plug_in_DB as mcInputDB
from common.module.Output.MainCardOutput import plug_in as mcOutput, plug_in_DB as mcOutputDB
from common.module.Transform.MainCardDataframe import main_cardDF, disk_usage ,memory_usage, os_counts, wired_counts, virtual_counts, daily_os

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
    # ------------------------------------- SBOM -----------------------------------------
    sbomOutputData = SbomDF('list')
    SbomOut(sbomOutputData, 'sbom_list')
    sbomOutputDataDetail = SbomDF('detail')
    SbomOut(sbomOutputDataDetail, 'sbom_detail')

    # ------------------------------------- 하단 최대 CPU 프로세스 --------------------------
    highCpuProcessInputData = highCpuProcInput()
    highCpuProcDF = highCpuProc_transform(highCpuProcessInputData)
    highCpuProcOutput(highCpuProcDF)
    # ------------------- 상단 메인 카드 -----------------------
    mainCardInputData = mcInput()
    maincardDF = main_cardDF(mainCardInputData)
    DU = disk_usage(maincardDF)
    MU = memory_usage(maincardDF)
    ON = os_counts(maincardDF)
    WC = wired_counts(maincardDF)
    VI = virtual_counts(maincardDF)
    mcOutput(maincardDF, 'asset')  # asset data 처리
    mcOutput(None, 'statistics', DU, MU, ON, WC, VI)

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

    # -----------------------------예상/유휴자산 ------------------------------------
    idleOutputData=IdleDF()
    IdleOut(idleOutputData, 'asset')
    idleInputData = plug_in_DB()
    IdleOut(idleInputData, 'statistics')
    # -----------------------------중앙 관리 라인차트 ----------------------------------
    osOriginData = mcInputDB()
    mcOutputDB(osOriginData)
    # -----------------------------중앙 미관리 라인차트 ----------------------------------
    disOriginData = disInput()
    disOut(disOriginData)
    # ----------------------------- 하단 OM Report -----------------------------------------
    reportInputData = reportInput()
    reportTransformDiscover = report_transform(reportInputData, 'DISCOVER_RESULT')
    reportTransformIdle = report_transform(reportInputData, 'IDLE_RESULT')
    reportTransformSubnetIsvm = report_transform(reportInputData, 'SUBNET_ISVM_RESULT')
    reportOut(reportTransformDiscover, 'DISCOVER_RESULT')
    reportOut(reportTransformIdle, 'IDLE_RESULT')
    reportOut(reportTransformSubnetIsvm, 'SUBNET_ISVM_RESULT')

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