### 분석연월 데이터 마트 추출 ###

import pandas as pd
# makeImpDataMart : 수출입 데이터 마트를 만드는 함수
# impHsDF : 수출입 데이터
# impCaterHsCdMapDF : 수출입 풍목코드 대비 요식업 업종 매핑 테이블 데이터
def makeImpDataMart(impHsDF, impCaterHsCdMapDF, logger, groupCol=["DATE", "HS_CODE"]):
    try:
        logger.info("수출입 데이터 마트 추출")
        # 분석연월, 품목코드 별 갯수, 매입, 중량 합 추출
        cateringHsCdGrDF = impHsDF.groupby(groupCol).\
            agg({"IMPRT_NMBR_TRNSC": "sum", "IMPRT_WGHT_KG": "sum", "IMPRT_AMT_USD": "sum", "IMPRT_BIZ_COUNT":"sum"}).\
            reset_index(drop=False)
        # 매핑테이블에 적용
        impDataMartPre = pd.merge(impCaterHsCdMapDF, cateringHsCdGrDF, on=["HS_CODE"])
        # 적용 후 키 값에 따른 합 추출
        impDataMart = impDataMartPre.groupby(["DATE", "THS_CD", "THS_NM", "UHS_CD", "UHS_NM", "HS_CODE", "HS_NM"]). \
            agg({"IMPRT_NMBR_TRNSC": "sum", "IMPRT_WGHT_KG": "sum", "IMPRT_AMT_USD": "sum", "IMPRT_BIZ_COUNT":"sum"}).\
            reset_index(drop=False)
        logger.info("수출입 데이터 마트 추출 완료")
        return impDataMart
    except Exception as e:
        logger.error(e)
        raise
# makeLclDataMart : 로컬 데이터 마트를 만드는 함수
# lclCateringCntDF : 로컬데이터 기준 시도/시군구별 요식업종 업체 수 데이터
# lclCaterMapDF : 로컬데이터 업종 대비 요식업종 매핑 테이블
# areaCodeDF : 시도/시군구 코드 매핑 테이블
def makeLclDataMart(lclCateringCntDF, lclCaterMapDF, areaCodeDF, logger):
    try:
        logger.info("로컬 데이터 마트 추출")
        areaCodeDF["SIDO_SGG"] = areaCodeDF["SIDO"] + " " + areaCodeDF["SGG"]
        lclDataMartPre1 = pd.merge(lclCateringCntDF, lclCaterMapDF, on=["mainCategory", "opnSvcNm", "uptaeNm"])
        lclDataMartPre2 = pd.merge(lclDataMartPre1, areaCodeDF, on=["SGG_CODE"])
        lclDataMart = lclDataMartPre2.groupby(["DATE", "SIDO_SGG", "SIDO_CODE", "SIDO", "SGG_CODE", "SGG", "THS_NM"]).\
            agg({"comCount": "sum"}).\
            reset_index(drop=False)
        logger.info("로컬 데이터 마트 추출 완료")
        return lclDataMart
    except Exception as e:
        logger.error(e)
        raise
