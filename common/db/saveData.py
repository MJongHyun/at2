### 데이터 마트 및 결과 저장 코드 ###

import common.db.dbProp as dbProp
import common.etc.etcFunc as etcFunc
# saveImpDataMart : 분석연월 수출입 데이터 마트 저장
# impMartData : 분석연월 수출입 데이터 마트
def saveImpDataMart(impMartData, logger):
    try:
        logger.info("분석연월 수출입 데이터 마트 저장 시작")
        # DB 접속
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(impMartData, "IMP_DATA_MART", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 수출입 데이터 마트 저장 완료")
        return print("수출입 데이터 마트 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveLclDataMart : 분석연월 로컬 데이터 마트 저장
# lclMartData : 분석연월 로컬 데이터 마트
def saveLclDataMart(lclMartData, logger):
    try:
        logger.info("분석연월 로컬 데이터 마트 저장 시작")
        # DB 접속
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(lclMartData, "LCL_DATA_MART", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 로컬 데이터 마트 저장 완료")
        return print("분석연월 로컬 데이터 마트 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveCorrRes : 분석연월 상관계수 결과 저장
# allCorrDF : 분석연월 상관계수 결과
def saveCorrRes(allCorrDF, logger):
    try:
        logger.info("분석연월 결과데이터 저장 시작")
        # DB 접속
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(allCorrDF, "CRL_RST_FOD_ING", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 결과데이터 저장 완료")
        return print("분석연월 결과데이터 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveExpectRes : 분석연월 예측결과 저장
# expectDF : 분석연월 예측결과
def saveExpectRes(expectDF, logger):
    try:
        logger.info("분석연월 결과데이터 저장 시작")
        # DB 접속
        conn = dbProp.atLoadSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(expectDF, "EXP_RES_FOD_ING", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 결과데이터 저장 완료")
        return print("분석연월 결과데이터 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveCmnFoodIdxDF : 분석연월 공통 식재료 지수 저장
# cmnFoodIdxDF : 분석연월 공통 식재료 지수 결과
def saveCmnFoodIdxDF(cmnFoodIdxDF, logger):
    try:
        logger.info("분석연월 공통식재료 지수 저장 시작")
        # DB 접속
        conn = dbProp.atFinalSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(cmnFoodIdxDF, "TB_TS_TCF_CCCI", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 공통식재료 지수 저장 완료")
        return print("분석연월 공통식재료 지수 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveCmnFoodSetIdxDF : 분석연월 공통식재료 포함 요식업종 지수 저장
# cmnFoodIdxSetDF : 분석연월 공통식재료 포함 요식업종 지수 결과
def saveCmnFoodSetIdxDF(cmnFoodIdxSetDF, logger):
    try:
        logger.info("분석연월 공통식재료 포함 요식업종 지수 저장 시작")
        # DB 접속
        conn = dbProp.atFinalSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(cmnFoodIdxSetDF, "TB_TS_TRTMF_CCCI", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 공통식재료 포함 요식업종 지수 저장 완료")
        return print("분석연월 공통식재료 포함 요식업종 지수 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveCtrFoodIdxDF : 분석연월 요식업 식재료 지수 저장
# ctrFoodIdxDF : 분석연월 공통 식재료 지수 결과
def saveCtrFoodIdxDF(ctrFoodIdxDF, logger):
    try:
        logger.info("분석연월 요식업 식재료 지수 저장 시작")
        # DB 접속
        conn = dbProp.atFinalSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(ctrFoodIdxDF, "TB_TS_TRTF_CCCI", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 요식업 식재료 지수 저장 완료")
        return print("분석연월 요식업 식재료 지수 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveCtrFoodSetIdxDF : 분석연월 요식업종 지수 저장
# ctrFoodIdxSetDF : 분석연월 요식업종 지수 결과
def saveCtrFoodSetIdxDF(ctrFoodIdxSetDF, logger):
    try:
        logger.info("분석연월 요식업종 지수 저장 시작")
        # DB 접속
        conn = dbProp.atFinalSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(ctrFoodIdxSetDF, "TB_TS_TRTCFE_MF_CCCI", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 요식업종 지수 저장 완료")
        return print("분석연월 요식업종 지수 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveCmnFoodExpDF : 분석연월 공통 식재료 예측지수 저장
# cmnFoodExpDF : 분석연월 공통 식재료 예측지수 결과
def saveCmnFoodExpDF(cmnFoodExpDF, logger):
    try:
        logger.info("분석연월 공통 식재료 예측지수 저장 시작")
        # DB 접속
        conn = dbProp.atFinalSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(cmnFoodExpDF, "TB_TS_TCF_DPI", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 공통 식재료 예측지수 저장 완료")
        return print("분석연월 공통 식재료 예측지수 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveCmnFoodSetExpDF : 분석연월 공통식재료 포함 요식업종 예측지수 저장
# cmnFoodExpSetDF : 분석연월 공통식재료 포함 요식업종 예측지수 결과
def saveCmnFoodSetExpDF(cmnFoodExpSetDF, logger):
    try:
        logger.info("분석연월 공통식재료 포함 요식업종 예측지수 저장 시작")
        # DB 접속
        conn = dbProp.atFinalSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(cmnFoodExpSetDF, "TB_TS_TRTMF_CPI", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 공통식재료 포함 요식업종 예측지수 저장 완료")
        return print("분석연월 공통식재료 포함 요식업종 예측지수 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveCtrFoodExpDF : 분석연월 요식업 식재료 지수 저장
# ctrFoodExpDF : 분석연월 요식업 식재료 지수 결과
def saveCtrFoodExpDF(ctrFoodExpDF, logger):
    try:
        logger.info("분석연월 요식업 식재료 지수 저장 시작")
        # DB 접속
        conn = dbProp.atFinalSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(ctrFoodExpDF, "TB_TS_TRTF_CCCPI", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 요식업 식재료 지수 저장 완료")
        return print("분석연월 요식업 식재료 지수 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveCtrFoodSetExpDF : 분석연월 요식업종 지수 저장
# ctrFoodExpSetDF : 분석연월 요식업종 지수 결과
def saveCtrFoodSetExpDF(ctrFoodExpSetDF, logger):
    try:
        logger.info("분석연월 요식업종 지수 저장 시작")
        # DB 접속
        conn = dbProp.atFinalSaveDBProp(logger)
        cursor = conn.cursor()
        # 테이블 저장
        insertQuery = etcFunc.extractInsertQuery(ctrFoodExpSetDF, "TB_TS_TRTCFE_MF_CCCPI", logger)
        cursor.execute(insertQuery)
        conn.commit()
        conn.close()
        logger.info("분석연월 요식업종 지수 저장 완료")
        return print("분석연월 요식업종 지수 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveAT2CorrDF : 분석연월 at결과 상관계수값 저장
# allCorrDF : 분석연월 at결과 상관계수값 결과
def saveAT2CorrDF(allCorrDF, logger):
    try:
        logger.info("분석연월 AT 상관지수결과 저장 시작")
        resCol1 = ['DATE', 'SIDO_CODE', 'SGG_CODE', 'RST_CD', 'HS_CODE', 'SIDO', 'SGG', 'RST_NM', 'HS_DESC', 'HS_NM',
                   'IMPRT_AMT_CORR_IDX']
        resCol2 = ['DATE', 'SIDO_CODE', 'SGG_CODE', 'RST_CD', 'SIDO', 'SGG', 'RST_NM', 'HS_DESC', 'IMPRT_AMT_CORR_IDX']

        tableCol1 = ['BASE_YM', 'CTNP_CODE', 'SGGU_CODE', 'RBIZ_TOID_SE_CODE', 'HS_CODE', 'CTNP_NM', 'SGGU_NM',
                   'RBIZ_TOID_SE_NM', 'PDLT_SE_NM', "PDLT_NM", 'CMMN_FDINGR_CSP_CYC_CHG_IDEX']
        tableCol2 = ['BASE_YM', 'CTNP_CODE', 'SGGU_CODE', 'RBIZ_TOID_SE_CODE', 'CTNP_NM', 'SGGU_NM',
                     'RBIZ_TOID_SE_NM', 'PDLT_SE_NM', 'MAIN_FDINGR_CSP_CYC_CHG_IDEX']
        tableCol3 = ['BASE_YM', 'CTNP_CODE', 'SGGU_CODE', 'RBIZ_TOID_SE_CODE', 'HS_CODE', 'CTNP_NM', 'SGGU_NM',
                     'RBIZ_TOID_SE_NM', 'PDLT_SE_NM', "PDLT_NM", 'RBIZ_FDINGR_CSP_CHG_IDEX']
        tableCol4 = ['BASE_YM', 'CTNP_CODE', 'SGGU_CODE', 'RBIZ_TOID_SE_CODE', 'CTNP_NM', 'SGGU_NM',
                     'RBIZ_TOID_SE_NM', 'PDLT_SE_NM', 'CMMN_ELSE_FDINGR_CSP_CHG_IDEX']

        mask1 = (allCorrDF.RST_CD.str.contains("THS")) & (allCorrDF.HS_CODE != "")
        mask2 = (allCorrDF.RST_CD.str.contains("THS")) & (allCorrDF.HS_CODE == "")
        mask3 = (allCorrDF.RST_CD.str.contains("UHS")) & (allCorrDF.HS_CODE != "")
        mask4 = (allCorrDF.RST_CD.str.contains("UHS")) & (allCorrDF.HS_CODE == "")

        cmnFoodIdxDF = allCorrDF[mask1][resCol1].reset_index(drop = True).copy()
        cmnFoodIdxDF.columns = tableCol1
        cmnFoodIdxDF["CMMN_FDINGR_CSP_CYC_CHG_IDEX"] = round(cmnFoodIdxDF["CMMN_FDINGR_CSP_CYC_CHG_IDEX"], 2)

        cmnFoodIdxSetDF = allCorrDF[mask2][resCol2].reset_index(drop = True).copy()
        cmnFoodIdxSetDF.columns = tableCol2
        cmnFoodIdxSetDF["MAIN_FDINGR_CSP_CYC_CHG_IDEX"] = round(cmnFoodIdxSetDF["MAIN_FDINGR_CSP_CYC_CHG_IDEX"], 2)

        ctrFoodIdxDF = allCorrDF[mask3][resCol1].reset_index(drop = True).copy()
        ctrFoodIdxDF.columns = tableCol3
        ctrFoodIdxDF["RBIZ_FDINGR_CSP_CHG_IDEX"] = round(ctrFoodIdxDF["RBIZ_FDINGR_CSP_CHG_IDEX"], 2)

        ctrFoodIdxSetDF = allCorrDF[mask4][resCol2].reset_index(drop = True).copy()
        ctrFoodIdxSetDF.columns = tableCol4
        ctrFoodIdxSetDF["CMMN_ELSE_FDINGR_CSP_CHG_IDEX"] = round(ctrFoodIdxSetDF["CMMN_ELSE_FDINGR_CSP_CHG_IDEX"], 2)

        saveCmnFoodIdxDF(cmnFoodIdxDF, logger)
        saveCmnFoodSetIdxDF(cmnFoodIdxSetDF, logger)
        saveCtrFoodIdxDF(ctrFoodIdxDF, logger)
        saveCtrFoodSetIdxDF(ctrFoodIdxSetDF, logger)

        logger.info("분석연월AT 상관지수결과 저장 완료")
        return print("분석연월AT 상관지수결과 저장 완료")
    except Exception as e:
        logger.error(e)
        raise

# saveAT2expDF : 분석연월 at결과 예측값 저장
# expResultDF : 분석연월 at결과 예측값 결과
def saveAT2expDF(expResultDF, logger):
    try:
        logger.info("분석연월 AT 상관지수결과 저장 시작")
        resCol1 = ['DATE', 'SIDO_CODE', 'SGG_CODE', 'RST_CD', 'HS_CODE', 'SIDO', 'SGG', 'RST_NM', 'HS_DESC', 'HS_NM',
                   'IMPRT_AMT_EXP_IDX']
        resCol2 = ['DATE', 'SIDO_CODE', 'SGG_CODE', 'RST_CD', 'SIDO', 'SGG', 'RST_NM', 'HS_DESC', 'IMPRT_AMT_EXP_IDX']

        tableCol1 = ['BASE_YM', 'CTNP_CODE', 'SGGU_CODE', 'RBIZ_TOID_SE_CODE', 'HS_CODE', 'CTNP_NM', 'SGGU_NM',
                     'RBIZ_TOID_SE_NM', 'PDLT_SE_NM', "PDLT_NM", 'CMMN_FDINGR_DMD_PRDT_IDEX']
        tableCol2 = ['BASE_YM', 'CTNP_CODE', 'SGGU_CODE', 'RBIZ_TOID_SE_CODE', 'CTNP_NM', 'SGGU_NM',
                     'RBIZ_TOID_SE_NM', 'PDLT_SE_NM', 'MAIN_FDINGR_CSP_PRDT_IDEX']
        tableCol3 = ['BASE_YM', 'CTNP_CODE', 'SGGU_CODE', 'RBIZ_TOID_SE_CODE', 'HS_CODE', 'CTNP_NM', 'SGGU_NM',
                     'RBIZ_TOID_SE_NM', 'PDLT_SE_NM', "PDLT_NM", 'RBIZ_FDINGR_CSP_PRDT_IDEX']
        tableCol4 = ['BASE_YM', 'CTNP_CODE', 'SGGU_CODE', 'RBIZ_TOID_SE_CODE', 'CTNP_NM', 'SGGU_NM',
                     'RBIZ_TOID_SE_NM', 'PDLT_SE_NM', 'CMMN_ELSE_FDINGR_CSP_PRDT_IDEX']

        mask1 = (expResultDF.RST_CD.str.contains("THS")) & (expResultDF.HS_CODE != "")
        mask2 = (expResultDF.RST_CD.str.contains("THS")) & (expResultDF.HS_CODE == "")
        mask3 = (expResultDF.RST_CD.str.contains("UHS")) & (expResultDF.HS_CODE != "")
        mask4 = (expResultDF.RST_CD.str.contains("UHS")) & (expResultDF.HS_CODE == "")

        cmnFoodExpDF = expResultDF[mask1][resCol1].reset_index(drop = True).copy()
        cmnFoodExpDF.columns = tableCol1
        cmnFoodExpDF['CMMN_FDINGR_DMD_PRDT_IDEX'] = round(cmnFoodExpDF['CMMN_FDINGR_DMD_PRDT_IDEX'], 2)

        cmnFoodExpSetDF = expResultDF[mask2][resCol2].reset_index(drop = True).copy()
        cmnFoodExpSetDF.columns = tableCol2
        cmnFoodExpSetDF['MAIN_FDINGR_CSP_PRDT_IDEX'] = round(cmnFoodExpSetDF['MAIN_FDINGR_CSP_PRDT_IDEX'], 2)

        ctrFoodExpDF = expResultDF[mask3][resCol1].reset_index(drop = True).copy()
        ctrFoodExpDF.columns = tableCol3
        ctrFoodExpDF['RBIZ_FDINGR_CSP_PRDT_IDEX'] = round(ctrFoodExpDF['RBIZ_FDINGR_CSP_PRDT_IDEX'], 2)

        ctrFoodExpSetDF = expResultDF[mask4][resCol2].reset_index(drop = True).copy()
        ctrFoodExpSetDF.columns = tableCol4
        ctrFoodExpSetDF['CMMN_ELSE_FDINGR_CSP_PRDT_IDEX'] = round(ctrFoodExpSetDF['CMMN_ELSE_FDINGR_CSP_PRDT_IDEX'], 2)

        saveCmnFoodExpDF(cmnFoodExpDF, logger)
        saveCmnFoodSetExpDF(cmnFoodExpSetDF, logger)
        saveCtrFoodExpDF(ctrFoodExpDF, logger)
        saveCtrFoodSetExpDF(ctrFoodExpSetDF, logger)

        logger.info("분석연월 요식업식재료 예측지수 저장 완료")
        return print("분석연월 예측지수데이터 저장 완료")

    except Exception as e:
        logger.error(e)
        raise

