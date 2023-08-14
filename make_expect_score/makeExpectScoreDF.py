### 상관계수를 통해 예측지수 추출하는 코드###

import numpy as np
import pandas as pd
from common.db.getCorrData import *
# makeExpectScoreData : 상관계수를 통해 예측지수 추출하는 함수
# corrResult : 분석연월, 분석연월 전달의 상관계수 결과 값
# dt : 분석연월
def makeExpectScoreData(corrResult, dt, logger):
    try:
        logger.info("분석연월 " + dt + "에 대한 예측값 추출")
        # key값과 결과값에 대한 컬럼 추출
        resList = []
        corrDFResCol = ["IMPRT_CNT_CORR_IDX", "IMPRT_WGHT_CORR_IDX", "IMPRT_AMT_CORR_IDX", "IMPRT_BIZ_CNT_CORR_IDX"]
        corrDFKeyCol = ['SIDO_CODE', 'SGG_CODE', 'RST_CD', 'HS_CODE', 'SIDO', 'SGG', 'RST_NM', 'HS_DESC', 'HS_NM']
        # 전달 이번달 모두 존재하는 key값에 대해 전달 대비 분석연월 상관계수의 각 차이값 추출
        # 이번달에만 존재할 경우 * 100 해서 값 적용
        for key, res in corrResult.groupby(corrDFKeyCol):
            if res.DATE.max() != dt:
                continue
            else:
                keyList = [dt]
                keyList.extend(list(key))
                onlyValue = res[corrDFResCol]
                if len(onlyValue) == 1:
                    value = res[corrDFResCol].iloc[-1].values * 100
                    keyList.extend(value)
                    resList.append(keyList)
                else:
                    value = res[corrDFResCol].diff(1).iloc[-1].values * 100
                    keyList.extend(value)
                    resList.append(keyList)
        # 결과데이터 추출
        resCol = ['DATE', 'SIDO_CODE', 'SGG_CODE', 'RST_CD', 'HS_CODE', 'SIDO', 'SGG', 'RST_NM', 'HS_DESC', 'HS_NM',
                  'IMPRT_CNT_EXP_IDX', 'IMPRT_WGHT_EXP_IDX', 'IMPRT_AMT_EXP_IDX', 'IMPRT_BIZ_CNT_EXP_IDX']
        resultDF = pd.DataFrame(resList, columns = resCol).reset_index(drop = True)
        logger.info("분석연월 " + dt + "에 대한 예측값 추출 완료")
        return resultDF

    except Exception as e:
        logger.error(e)
        raise