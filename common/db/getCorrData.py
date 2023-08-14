### 분석연월과 그 전달 상관계수 데이터 불러오는 함수 ###

import common.db.dbProp as dbProp
import common.etc.etcFunc as etcFunc
import pandas as pd
# loadCorrDF : 상관계수 결과 데이터 불러오는 함수
def loadCorrDF(dt, logger):
    try:
        logger.info("분석연월" + str(dt) + " 대비 전달, 이번달 상관계수 결과 데이터 추출")
        # 분석연월 대비 1년치 분석연월 불러오기
        dtList = etcFunc.makeDtList(dt, logger)[0:2]
        strDtList = str(tuple(dtList))
        # DB 접속
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        # 데이터 추출
        sql = """SELECT * FROM CRL_RST_FOD_ING WHERE DATE in {}""".format(strDtList)
        cursor.execute(sql)
        loadData = cursor.fetchall()
        corrDFData = pd.DataFrame(loadData)
        conn.close()
        logger.info("분석연월" + str(dt) + " 대비 전달, 이번달 상관계수 결과 데이터 추출완료")
        return corrDFData
    except Exception as e:
        logger.error(e)
        raise
