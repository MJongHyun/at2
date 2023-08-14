### 상관관계를 추출하기 위한 데이터 마트 추출 ####

import pandas as pd
# findAllDealHscode : 1년치 수출입 데이터 마트를 통해 1년동안 계속 거래한 품목추출
# impDataMart : 1년치 수출입 데이터 마
def findAllDealHscode(impDataMart, logger):
    try:
        logger.info("분석연월 대비 1년 동안 계속 거래한 품목코드 추출")
        allDealDT = impDataMart.DATE.unique()
        hsCode = impDataMart[["DATE", "HS_CODE"]].drop_duplicates()
        hsCodeDF = hsCode.groupby(["HS_CODE"]).count().reset_index(drop=False)
        allHsCode = hsCodeDF[hsCodeDF["DATE"] == len(allDealDT)][["HS_CODE"]]
        logger.info("분석연월 대비 1년 동안 계속 거래한 품목코드 추출 완료")
        return allHsCode
    except Exception as e:
        logger.error(e)
        raise

# makeCorrDataMart1 : 지역별 업종별 인허가 시계열 데이터와 공통 식재료 개별 HS 코드별 시계열 데이터 Join (금액, 건수, 업체수, 중량)
# impDataMart : 1년치 수출입 데이터 마트
# lclDataMart : 분석연월 로컬 데이터 마트
# allHsCode : 1년동안 계속 거래한 품목코드 리스트
def makeCorrDataMart1(impDataMart, lclDataMart, allHsCode, logger):
    try:
        logger.info("지역별 업종별 인허가 시계열 데이터와 공통 식재료 개별 HS 코드별 시계열 데이터 Join (금액, 건수, 업체수, 중량), mart1")
        impDataMart["DATE"] = impDataMart["DATE"].astype(str)
        lclDataMart["DATE"] = lclDataMart["DATE"].astype(str)
        allHsCodeList = allHsCode.HS_CODE.tolist()

        typeNm = "공통식재료"
        lclCommDF = lclDataMart[lclDataMart["THS_NM"] == typeNm].copy()
        impCommDF = impDataMart[(impDataMart["THS_NM"] == typeNm) & (impDataMart["HS_CODE"].isin(allHsCodeList))].copy()

        lclCommPivotDF = pd.pivot_table(lclCommDF, \
                                        index=['DATE', 'THS_NM'], \
                                        values='comCount', \
                                        columns=['SIDO_SGG'], \
                                        aggfunc='sum'). \
            reset_index(drop=False). \
            fillna(0)

        impCommGroupSumDF = impCommDF.groupby(["DATE", "THS_CD", "THS_NM", "HS_CODE", "HS_NM"]). \
            agg({"IMPRT_NMBR_TRNSC": "sum", "IMPRT_WGHT_KG": "sum", "IMPRT_AMT_USD": "sum", "IMPRT_BIZ_COUNT":"sum"}).\
            reset_index(drop=False)
        impCommGroupSumDF.columns = ["DATE", "THS_CD", "THS_NM", "HS_CODE", "HS_NM", "IMPRT_CNT_SUM",
                                     "IMPRT_WGHT_SUM", "IMPRT_AMT_SUM", "IMPRT_BIZ_CNT_SUM"]

        commonMartData = pd.merge(lclCommPivotDF, impCommGroupSumDF, on=["DATE", "THS_NM"])
        logger.info("mart1 추출 완료")
        return commonMartData
    except Exception as e:
        logger.error(e)
        raise

# makeCorrDataMart2 : 지역별 업종별 인허가 시계열 데이터와 해당 업종에 대응되는 개별 HS 코드별 - 공통식재료 HS 코드 시계열 데이터 Join (금액, 건수, 업체수, 중량)
# impDataMart : 1년치 수출입 데이터 마트
# lclDataMart : 분석연월 로컬 데이터 마트
# allHsCode : 1년동안 계속 거래한 품목코드 리스트
def makeCorrDataMart2(impDataMart, lclDataMart, allHsCode, logger):
    try:
        logger.info("지역별 업종별 인허가 시계열 데이터와 해당 업종에 대응되는 개별 HS 코드별 - 공통식재료 HS 코드 시계열 데이터 Join (금액, 건수, 업체수, 중량), mart2")
        impDataMart["DATE"] = impDataMart["DATE"].astype(str)
        lclDataMart["DATE"] = lclDataMart["DATE"].astype(str)
        allHsCodeList = allHsCode.HS_CODE.tolist()

        typeNm = "공통식재료"
        lclUhsCdDF = lclDataMart[lclDataMart["THS_NM"] != typeNm].copy()
        impUhsCdDF = impDataMart[(impDataMart["UHS_NM"] != typeNm) & (impDataMart["HS_CODE"].isin(allHsCodeList))].copy()
        impUhsCdDF["THS_CD"] = impUhsCdDF["UHS_CD"]

        lclUhsPivotDF = pd.pivot_table(lclUhsCdDF, \
                                       index=['DATE', 'THS_NM'], \
                                       values='comCount', \
                                       columns=['SIDO_SGG'], \
                                       aggfunc='sum'). \
            reset_index(drop=False). \
            fillna(0)

        impUhsGroupSumDF = impUhsCdDF.groupby(["DATE", "THS_CD", "THS_NM", "HS_CODE", "HS_NM"]). \
            agg({"IMPRT_NMBR_TRNSC": "sum", "IMPRT_WGHT_KG": "sum", "IMPRT_AMT_USD": "sum", "IMPRT_BIZ_COUNT":"sum"}).\
            reset_index(drop=False)
        impUhsGroupSumDF.columns = ["DATE", "THS_CD", "THS_NM", "HS_CODE", "HS_NM", "IMPRT_CNT_SUM",
                                    "IMPRT_WGHT_SUM", "IMPRT_AMT_SUM", "IMPRT_BIZ_CNT_SUM"]
        hsCdMartData = pd.merge(lclUhsPivotDF, impUhsGroupSumDF, on=["DATE", "THS_NM"])
        logger.info("mart2 추출 완료")
        return hsCdMartData
    except Exception as e:
        logger.error(e)
        raise

# makeCorrDataMart3 : 지역별 업종별 인허가 시계열 데이터와 해당 업종에 대응되는 HS 코드셋의 sum 시계열 데이터 Join (금액, 건수, 업체수, 중량)
# impDataMart : 1년치 수출입 데이터 마트
# lclDataMart : 분석연월 로컬 데이터 마트
# allHsCode : 1년동안 계속 거래한 품목코드 리스트
def makeCorrDataMart3(impDataMart, lclDataMart, allHsCode, logger):
    try:
        logger.info("지역별 업종별 인허가 시계열 데이터와 해당 업종에 대응되는 HS 코드셋의 sum, mean 시계열 데이터 Join (금액, 건수, 업체수, 중량), mart3")
        impDataMart["DATE"] = impDataMart["DATE"].astype(str)
        lclDataMart["DATE"] = lclDataMart["DATE"].astype(str)
        allHsCodeList = allHsCode.HS_CODE.tolist()

        allDealImpMartDF = impDataMart[impDataMart["HS_CODE"].isin(allHsCodeList)].copy()

        impAllGrSumDF = allDealImpMartDF.groupby(["DATE", "THS_CD", "THS_NM"]). \
            agg({"IMPRT_NMBR_TRNSC": "sum", "IMPRT_WGHT_KG": "sum", "IMPRT_AMT_USD": "sum", "IMPRT_BIZ_COUNT":"sum"}).\
            reset_index(drop=False)
        impAllGrSumDF.columns = ["DATE", "THS_CD", "THS_NM", "IMPRT_CNT_SUM", "IMPRT_WGHT_SUM",
                                 "IMPRT_AMT_SUM", "IMPRT_BIZ_CNT_SUM"]
        lclPivotDF = pd.pivot_table(lclDataMart, \
                                    index=['DATE', 'THS_NM'], \
                                    values='comCount', \
                                    columns=['SIDO_SGG'], \
                                    aggfunc='sum'). \
            reset_index(drop=False). \
            fillna(0)

        impAllCaterGrDF = pd.merge(impAllGrSumDF, lclPivotDF, on=["DATE", "THS_NM"])
        logger.info("mart3 추출 완료")
        return impAllCaterGrDF
    except Exception as e:
        logger.error(e)
        raise

# makeCorrDataMart4 : 지역별 업종별 인허가 시계열 데이터와 해당 업종에 대응되는 HS 코드셋 - 공통식재료 HS 코드의 sum 시계열 데이터 Join (금액, 건수, 업체수, 중량)
# impDataMart : 1년치 수출입 데이터 마트
# lclDataMart : 분석연월 로컬 데이터 마트
# allHsCode : 1년동안 계속 거래한 품목코드 리스트
def makeCorrDataMart4(impDataMart, lclDataMart, allHsCode, logger):
    try:
        logger.info("지역별 업종별 인허가 시계열 데이터와 해당 업종에 대응되는 HS 코드셋 - 공통식재료 HS 코드의 sum, mean 시계열 데이터 Join (금액, 건수, 업체수, 중량), mart4")
        impDataMart["DATE"] = impDataMart["DATE"].astype(str)
        lclDataMart["DATE"] = lclDataMart["DATE"].astype(str)
        allHsCodeList = allHsCode.HS_CODE.tolist()

        lclUhsDF = lclDataMart[lclDataMart["THS_NM"] != "공통식재료"].copy()
        impUhsDF = impDataMart[(impDataMart["UHS_NM"] != "공통식재료") & (impDataMart["HS_CODE"].isin(allHsCodeList))].copy()
        impUhsDF["THS_CD"] = impUhsDF["UHS_CD"]

        impUhsNmGrSumDF = impUhsDF.groupby(["DATE", "THS_CD", "THS_NM"]).\
            agg({"IMPRT_NMBR_TRNSC": "sum", "IMPRT_WGHT_KG": "sum", "IMPRT_AMT_USD": "sum", "IMPRT_BIZ_COUNT":"sum"}).\
            reset_index(drop=False)
        impUhsNmGrSumDF.columns = ["DATE", "THS_CD", "THS_NM", "IMPRT_CNT_SUM", "IMPRT_WGHT_SUM",
                                   "IMPRT_AMT_SUM", "IMPRT_BIZ_CNT_SUM"]
        lclUhsPivotDF = pd.pivot_table(lclUhsDF, \
                                       index=['DATE', 'THS_NM'], \
                                       values='comCount', \
                                       columns=['SIDO_SGG'], \
                                       aggfunc='sum'). \
            reset_index(drop=False). \
            fillna(0)

        uhsAllGrDF = pd.merge(impUhsNmGrSumDF, lclUhsPivotDF, on=["DATE", "THS_NM"])
        logger.info("mart4 추출 완료")

        return uhsAllGrDF
    except Exception as e:
        logger.error(e)
        raise

