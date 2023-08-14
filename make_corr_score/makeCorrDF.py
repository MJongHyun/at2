### 상관계수 결과 값 추출 ###

import pandas as pd
import numpy as np
import re

# makeMart1CorrDF : mart1에 대한 상관계수 추출
# martData : mart1 - 지역별 업종별 인허가 시계열 데이터와 공통 식재료 개별 HS 코드별 시계열 데이터 Join (금액, 건수, 업체수, 중량)
# areaCodeDF : 시도/시군구 매핑 테이블
# dt : 분석연월
def makeMart1CorrDF(martData, areaCodeDF, dt, logger):
    try:
        logger.info("mart1에 대한 상관계수 추출")
        areaCodeDF["SIDO_SGG"] = areaCodeDF["SIDO"] + " " + areaCodeDF["SGG"]
        corrDFPre = pd.DataFrame()
        # 데이터의 key값 추출
        keyCode = martData[["HS_CODE", "HS_NM", "THS_CD", "THS_NM"]].drop_duplicates().copy()
        keyCodeList = np.array(keyCode).tolist()
        # key값에 따르는 개수, 매입액, 중량의 합, 평균 추출
        for hsCd, hsNm, rstCd, rstNm in keyCodeList:
            mask = martData["HS_CODE"] == hsCd
            hsCdData = martData[mask].copy()
            hsCdCorrDF = hsCdData.corr()[
                             ["IMPRT_CNT_SUM", "IMPRT_WGHT_SUM", "IMPRT_AMT_SUM", "IMPRT_BIZ_CNT_SUM"]]. \
                             reset_index(drop=False).iloc[:-4, :].fillna(0).round(4)
            hsCdCorrDF.columns = ["SIDO_SGG", "IMPRT_CNT_CORR_IDX",
             "IMPRT_WGHT_CORR_IDX", "IMPRT_AMT_CORR_IDX", "IMPRT_BIZ_CNT_CORR_IDX"]
            # 결과에 key값 적용 후 데이터 union
            hsCdCorrDF["RST_CD"] = rstCd
            hsCdCorrDF["RST_NM"] = rstNm
            hsCdCorrDF["HS_CODE"] = hsCd
            hsCdCorrDF["HS_NM"] = hsNm
            corrDFPre = pd.concat([corrDFPre, hsCdCorrDF], axis=0).fillna(0)
        # 컬럼에 맞게 데이터 추출
        corrDFPre2 = pd.merge(corrDFPre, areaCodeDF, on=["SIDO_SGG"])
        corrDFPre2["DATE"] = dt
        corrDF = corrDFPre2[
            ["DATE", "SIDO_CODE", "SGG_CODE", "RST_CD", "HS_CODE", "SIDO", "SGG", "RST_NM", "HS_NM", "IMPRT_CNT_CORR_IDX",
             "IMPRT_WGHT_CORR_IDX", "IMPRT_AMT_CORR_IDX", "IMPRT_BIZ_CNT_CORR_IDX"]].copy()
        logger.info("mart1에 대한 상관계수 추출 완료")
        return corrDF
    except Exception as e:
        logger.error(e)
        raise

# makeMart2CorrDF : mart2에 대한 상관계수 추출
# martData : mart2 - 지역별 업종별 인허가 시계열 데이터와 해당 업종에 대응되는 개별 HS 코드별 - 공통식재료 HS 코드 시계열 데이터 Join (금액, 건수, 업체수, 중량)
# areaCodeDF : 시도/시군구 매핑 테이블
# dt : 분석연월
def makeMart2CorrDF(martData, areaCodeDF, dt, logger):
    try:
        logger.info("mart2에 대한 상관계수 추출")
        areaCodeDF["SIDO_SGG"] = areaCodeDF["SIDO"] + " " + areaCodeDF["SGG"]
        corrDFPre = pd.DataFrame()
        # 데이터의 key값 추출
        keyCode = martData[["HS_CODE", "HS_NM", "THS_CD", "THS_NM"]].drop_duplicates().copy()
        keyCodeList = np.array(keyCode).tolist()
        # key값에 따르는 개수, 매입액, 중량의 합, 평균 추출
        for hsCd, hsNm, rstCd, rstNm in keyCodeList:
            mask = (martData["HS_CODE"] == hsCd) & (martData["THS_NM"] == rstNm)
            hsCdData = martData[mask].copy()
            hsCdCorrDF = hsCdData.corr()[
                             ["IMPRT_CNT_SUM", "IMPRT_WGHT_SUM", "IMPRT_AMT_SUM", "IMPRT_BIZ_CNT_SUM"]]. \
                             reset_index(drop=False).iloc[:-4, :].fillna(0).round(4)
            hsCdCorrDF.columns = ["SIDO_SGG", "IMPRT_CNT_CORR_IDX",
             "IMPRT_WGHT_CORR_IDX", "IMPRT_AMT_CORR_IDX", "IMPRT_BIZ_CNT_CORR_IDX"]
            # 결과에 key값 적용 후 데이터 union
            hsCdCorrDF["RST_CD"] = rstCd
            hsCdCorrDF["RST_NM"] = rstNm
            hsCdCorrDF["HS_CODE"] = hsCd
            hsCdCorrDF["HS_NM"] = hsNm
            corrDFPre = pd.concat([corrDFPre, hsCdCorrDF], axis=0).fillna(0)
        # 컬럼에 맞게 데이터 추출
        corrDFPre2 = pd.merge(corrDFPre, areaCodeDF, on=["SIDO_SGG"])
        corrDFPre2["DATE"] = dt
        corrDF = corrDFPre2[
            ["DATE", "SIDO_CODE", "SGG_CODE", "RST_CD", "HS_CODE", "SIDO", "SGG", "RST_NM", "HS_NM", "IMPRT_CNT_CORR_IDX",
             "IMPRT_WGHT_CORR_IDX", "IMPRT_AMT_CORR_IDX", "IMPRT_BIZ_CNT_CORR_IDX"]].copy()
        logger.info("mart2에 대한 상관계수 추출 완료")
        return corrDF
    except Exception as e:
        logger.error(e)
        raise

# makeMart3Or4CorrDF : mart3,4에 대한 상관계수 추출
# martData : mart3 - 지역별 업종별 인허가 시계열 데이터와 해당 업종에 대응되는 HS 코드셋의 sum 시계열 데이터 Join (금액, 건수, 업체수, 중량)
#            mart4 - 지역별 업종별 인허가 시계열 데이터와 해당 업종에 대응되는 HS 코드셋 - 공통식재료 HS 코드의 sum 시계열 데이터 Join (금액, 건수, 업체수, 중량)
# areaCodeDF : 시도/시군구 매핑 테이블
# dt : 분석연월
def makeMart3Or4CorrDF(martData, areaCodeDF, dt, logger):
    try:
        logger.info("mart3,4에 대한 상관계수 추출")
        areaCodeDF["SIDO_SGG"] = areaCodeDF["SIDO"] + " " + areaCodeDF["SGG"]
        corrDFPre = pd.DataFrame()
        # 데이터의 key값 추출
        keyCode = martData[["THS_CD", "THS_NM"]].drop_duplicates()
        keyCodeList = np.array(keyCode).tolist()
        # key값에 따르는 개수, 매입액, 중량의 합, 평균 추출
        for rstCd, rstNm in keyCodeList:
            mask = (martData["THS_CD"] == rstCd)
            thmMartDF = martData[mask]
            thmMartDFCorr = thmMartDF.corr()[
                                ["IMPRT_CNT_SUM", "IMPRT_WGHT_SUM", "IMPRT_AMT_SUM", "IMPRT_BIZ_CNT_SUM"]].\
                                reset_index(drop=False).iloc[4:, :].fillna(0).round(4)
            thmMartDFCorr.columns = ["SIDO_SGG", "IMPRT_CNT_CORR_IDX",
             "IMPRT_WGHT_CORR_IDX", "IMPRT_AMT_CORR_IDX", "IMPRT_BIZ_CNT_CORR_IDX"]
            # 결과에 key값 적용 후 데이터 union
            thmMartDFCorr["RST_CD"] = rstCd
            thmMartDFCorr["RST_NM"] = rstNm
            thmMartDFCorr["HS_CODE"] = ""
            thmMartDFCorr["HS_NM"] = ""
            corrDFPre = pd.concat([corrDFPre, thmMartDFCorr], axis=0).fillna(0)
        # 컬럼에 맞게 데이터 추출
        corrDFPre2 = pd.merge(corrDFPre, areaCodeDF, on=["SIDO_SGG"])
        corrDFPre2["DATE"] = dt
        corrDF = corrDFPre2[
            ["DATE", "SIDO_CODE", "SGG_CODE", "RST_CD", "HS_CODE", "SIDO", "SGG", "RST_NM", "HS_NM", "IMPRT_CNT_CORR_IDX",
             "IMPRT_WGHT_CORR_IDX", "IMPRT_AMT_CORR_IDX", "IMPRT_BIZ_CNT_CORR_IDX"]].copy()
        logger.info("mart3,4에 대한 상관계수 추출 완료")
        return corrDF
    except Exception as e:
        logger.error(e)
        raise
# makeAllcorrDF : 추출한 전체결과 합침
# corrDF* : mart*을 통해 추출한 결과들
def makeAllcorrDF(corrDF1, corrDF2, corrDF3, corrDF4, logger):
    try:
        logger.info("전체 결과 union")
        resCol = ['DATE', 'SIDO_CODE', 'SGG_CODE', 'RST_CD', 'HS_CODE', 'SIDO', 'SGG',
       'RST_NM', 'HS_DESC', 'HS_NM', "IMPRT_CNT_CORR_IDX", "IMPRT_WGHT_CORR_IDX",
                  "IMPRT_AMT_CORR_IDX", "IMPRT_BIZ_CNT_CORR_IDX"]
        allCorrDF = pd.concat([corrDF1, corrDF2, corrDF3, corrDF4], axis=0).reset_index(drop=True)
        # HS_DESC에 식재료 업종 관련 값 추출
        allCorrDF['HS_DESC'] = allCorrDF['RST_NM'].apply(lambda x: re.sub("(식재료)+", "식재료", x + "식재료"))
        allCorrDF.loc[allCorrDF["RST_NM"] == "공통식재료", "RST_NM"] = "음식점업"
        # 공통식재료가 포함되어 있는 것과 아닌 것을 구분하여 값 적용
        allCorrDF['HS_DESC'] = allCorrDF[['RST_CD', 'HS_DESC']].\
            apply(lambda x: x['HS_DESC'] + '(공통식재료포함)' if 'THS' in x['RST_CD'] else x['HS_DESC'] + '(공통식재료미포함)', axis=1)
        # 시작이 공통식재료인 값은 괄호값 제거
        allCorrDF['HS_DESC'] = allCorrDF['HS_DESC'].apply(lambda x: re.sub("\(.+\)", "", x) if re.search("^공통", x) else x)
        # HS_CODE가 빈 값이 아니라면 괄호값 제거
        allCorrDF['HS_DESC'] = allCorrDF[['HS_CODE', 'HS_DESC']].apply(
            lambda x: re.sub("\(.+\)", "", x['HS_DESC']) if x['HS_CODE'] != '' else x['HS_DESC'], axis=1)
        # 공통식재료 set인 경우 '음식점업식재료(공통식재료포함)' 값으로 변경
        allCorrDF.loc[(allCorrDF["HS_DESC"] == "공통식재료") & (allCorrDF["HS_CODE"] == "")
                      & (allCorrDF["RST_CD"].str.contains("THS")), "HS_DESC"] = "음식점업식재료(공통식재료포함)"
        resultDF = allCorrDF[resCol].copy()
        logger.info("전체 결과 union 완료")
        return resultDF
    except Exception as e:
        logger.error(e)
        raise
