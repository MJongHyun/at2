### 데이터 마트를 만들기 위한 ROW DATA 불러오기 ###

import common.db.dbProp as dbProp
from datetime import datetime
import pandas as pd
# loadImpHsData : 분석연월 수출입 데이터 추출
# dt : 분석연월
def loadImpHsData(dt, logger):
    try:
        logger.info(str(dt) + " 분석연월 수출입 데이터 불러오기")
        # 수출입 데이터의 경우, 날짜 컬럼이 '%Y-%m-%d %H:%M:%S' 형식이므로 변경 후 추출
        dt = str(dt)
        year = dt[:4]
        month = dt[4:]
        day = "01"
        date = year + "-" + month + "-" + day
        dateTimeDate = datetime.strptime(date, '%Y-%m-%d')
        strTime = dateTimeDate.strftime('%Y-%m-%d %H:%M:%S')
        # DB 접속
        conn = dbProp.impDBProp(logger)
        cursor = conn.cursor()
        # 해당 분석연월 값 추출
        sql = """SELECT * FROM joined_impHsBizCount WHERE DT = '{}'""".format(strTime)
        cursor.execute(sql)
        loadData = cursor.fetchall()
        conn.close()
        impHsDF = pd.DataFrame(loadData)
        impHsDF.columns = ["DATE", "HS_CODE", "HS_LEVEL", "HS_DESC", "IMPRT_NMBR_TRNSC", "IMPRT_WGHT_KG", "IMPRT_AMT_USD", "IMPRT_BIZ_COUNT"]
        impHsDF["DATE"] = dt
        logger.info(str(dt) + " 분석연월 수출입 데이터 불러오기 완료")
        return impHsDF
    except Exception as e:
        logger.error(e)
        raise

# loadLclCateringCntData : 분석연월 로컬데이터 업종 기준 시도/시군구 별 요식 업종 수 데이터 불러오기
# 현재 이 데이터의 경우, AT1차를 돌리지 않으면 결과가 생성되지 않으므로 반드시 실행을 해야함 (shell scrip 수정 필요)
# dt : 분석연월
def loadLclCateringCntData(dt, logger):
    try:
        logger.info(str(dt) + " 분석연월 지역별 요식업종 수 데이터 불러오기")
        dt = str(dt)
        # DB 접속
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        # 해당 분석연월 값 추출
        sql = """SELECT * FROM CATERING_SGG_COM_CNT WHERE DATE = {}""".format(dt)
        cursor.execute(sql)
        loadData = cursor.fetchall()
        conn.close()
        lclCateringCntDF = pd.DataFrame(loadData)
        logger.info(str(dt) + " 분석연월 지역별 요식업종 수 데이터 불러오기 완료")
        return lclCateringCntDF
    except Exception as e:
        logger.error(e)
        raise
