### 상관계수를 추출하기 위한 1년치 데이터 마트 추출 ###

import common.db.dbProp as dbProp
import common.etc.etcFunc as etcFunc
import pandas as pd
# loadLclDataMart : 1년치 로컬 데이터 마트 추출
# dt : 분석연월
def loadLclDataMart(dt, logger):
    try:
        logger.info("분석연월" + str(dt) + " 대비 1년치 로컬 데이터 마트 불러오기")
        # 분석연월 대비 1년치 분석연월 불러오기
        dtList = etcFunc.makeDtList(dt, logger)
        strDtList = str(tuple(dtList))
        # DB 접속
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        # 데이터 추출
        sql = """SELECT * FROM LCL_DATA_MART WHERE DATE in {}""".format(strDtList)
        cursor.execute(sql)
        loadData = cursor.fetchall()
        lclDataMart = pd.DataFrame(loadData)
        conn.close()
        logger.info("분석연월" + str(dt) + " 대비 1년치 로컬 데이터 마트 불러오기 완료")
        return lclDataMart
    except Exception as e:
        logger.error(e)
        raise
# loadImpDataMart : 1년치 수출입 데이터 마트 추출
# dt : 분석연월
def loadImpDataMart(dt, logger):
    try:
        logger.info("분석연월" + str(dt) + " 대비 1년치 수출입 데이터 마트 불러오기")
        # 분석연월 대비 1년치 분석연월 불러오기
        dtList = etcFunc.makeDtList(dt, logger)
        strDtList = str(tuple(dtList))
        # DB 접속
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        # 데이터 추출
        sql = """SELECT * FROM IMP_DATA_MART WHERE DATE in {}""".format(strDtList)
        cursor.execute(sql)
        loadData = cursor.fetchall()
        impDataMart = pd.DataFrame(loadData)
        conn.close()
        logger.info("분석연월" + str(dt) + " 대비 1년치 수출입 데이터 마트 불러오기 완료")
        return impDataMart
    except Exception as e:
        logger.error(e)
        raise

