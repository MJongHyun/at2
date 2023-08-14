### 실행 함수 ###

import sys
from common.db.getMappingData import *
from common.db.getMartData import *
from common.db.getRowData import *
from common.db.saveData import *
from common.db.getCorrData import *
from log.log import *
from make_mart.makeImpLclDataMart import *
from make_mart.makeCorrDataMart import *
from make_corr_score.makeCorrDF import *
from make_expect_score.makeExpectScoreDF import *

# main : 함수 실행
# dt : 분석연월
def main(dt):
    Log = log()
    logger = Log.logger
    print("분석연월 "+ str(dt) + " AT 2차 시작")
    logger.info("분석연월 "+ str(dt) + " AT 2차 시작")
    try:
        impHsDF = loadImpHsData(dt, logger)
        lclCateringCntDF = loadLclCateringCntData(dt, logger)
        areaCodeDF = loadSidoSggInfoData(logger)
        impCaterHsCdMapDF = loadImpCaterHsCdMapData(logger)
        lclCaterMapDF = loadLclCaterMapData(logger)

        lclDataMart = makeLclDataMart(lclCateringCntDF, lclCaterMapDF, areaCodeDF, logger)
        impDataMart = makeImpDataMart(impHsDF, impCaterHsCdMapDF, logger)

        saveImpDataMart(impDataMart, logger)
        saveLclDataMart(lclDataMart, logger)

        targetLoadImpDataMart = loadImpDataMart(dt, logger)
        targetLoadLclDataMart = loadLclDataMart(dt, logger)

        allHsCode = findAllDealHscode(targetLoadImpDataMart, logger)

        mart1 = makeCorrDataMart1(targetLoadImpDataMart, targetLoadLclDataMart, allHsCode, logger)
        mart2 = makeCorrDataMart2(targetLoadImpDataMart, targetLoadLclDataMart, allHsCode, logger)
        mart3 = makeCorrDataMart3(targetLoadImpDataMart, targetLoadLclDataMart, allHsCode, logger)
        mart4 = makeCorrDataMart4(targetLoadImpDataMart, targetLoadLclDataMart, allHsCode, logger)

        corrDF1 = makeMart1CorrDF(mart1, areaCodeDF, dt, logger)
        corrDF2 = makeMart2CorrDF(mart2, areaCodeDF, dt, logger)
        corrDF3 = makeMart3Or4CorrDF(mart3, areaCodeDF, dt, logger)
        corrDF4 = makeMart3Or4CorrDF(mart4, areaCodeDF, dt, logger)
        allCorrDF = makeAllcorrDF(corrDF1, corrDF2, corrDF3, corrDF4, logger)
        saveCorrRes(allCorrDF, logger)

        corrDFData = loadCorrDF(dt, logger)
        expResultDF = makeExpectScoreData(corrDFData, dt, logger)
        saveExpectRes(expResultDF, logger)

        saveAT2CorrDF(allCorrDF, logger)
        saveAT2expDF(expResultDF, logger)

        logger.info("분석연월 "+ str(dt) + " AT 2차 종료")
        return print("분석연월 "+ str(dt) + " AT 2차 종료")
    except Exception as e:
        print("실행 에러")
        logger.error(e)

if __name__ == '__main__':
    dt = sys.argv[1]
    main(dt)

