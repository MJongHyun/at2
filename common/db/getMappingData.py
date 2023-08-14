### 매핑테이블 불러오는 함수 ###

import common.db.dbProp as dbProp
import pandas as pd
# loadSidoSggInfoData : 시도/시군구 매핑테이블 추출
def loadSidoSggInfoData(logger):
    try:
        logger.info("시도 시군구 코드 정보 매핑 테이블 불러오기")
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        sql = """SELECT * FROM AREA_CODE"""
        cursor.execute(sql)
        loadData = cursor.fetchall()
        conn.close()
        areaCodeDF = pd.DataFrame(loadData)
        logger.info("시도 시군구 코드 정보 매핑 테이블 불러오기 완료")
        return areaCodeDF
    except Exception as e:
        logger.error(e)
        raise
# loadLclCaterMapData : 로컬데이터 기준 시도/시군구 대비 요식업종 업체 수 테이블 추출
def loadLclCaterMapData(logger):
    try:
        logger.info("요식업 매핑 테이블 불러오기")
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        sql = """SELECT * FROM CATERING_LCL_MAP_TBL"""
        cursor.execute(sql)
        loadData = cursor.fetchall()
        conn.close()
        LclCaterMapDF = pd.DataFrame(loadData)
        logger.info("요식업 매핑 테이블 불러오기 완료")
        return LclCaterMapDF
    except Exception as e:
        logger.error(e)
        raise
# loadLclCaterMapData : 품목별 요식업 매핑 테이블 추출
def loadImpCaterHsCdMapData(logger):
    try:
        logger.info("품목별 업종 매핑 테이블 불러오기")
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        sql = """SELECT * FROM CATERING_HSCD_MAP_TBL"""
        cursor.execute(sql)
        loadData = cursor.fetchall()
        conn.close()
        ImpCaterHsCdMapDF = pd.DataFrame(loadData)
        logger.info("품목별 업종 매핑 테이블 불러오기 완료")
        return ImpCaterHsCdMapDF
    except Exception as e:
        logger.error(e)
        raise
