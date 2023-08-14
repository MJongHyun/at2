### 이외에 사용하는 함수들 모음 ###

from datetime import datetime
from dateutil.relativedelta import relativedelta
# makeDtList : 분석연월 대비 지난 1년 분석연월 리스트 추출
# dt : 분석연월
def makeDtList(dt, logger):
    try:
        logger.info("1년 주기 추출")
        dtList = []
        dtList.append(dt)
        dateTime = datetime.strptime(dt, '%Y%m')
        for month in range(1, 12):
            bfDateTime = dateTime - relativedelta(months = month)
            bfDate = bfDateTime.strftime("%Y%m")
            dtList.append(bfDate)
        logger.info("1년 주기 추출 완료")
        return dtList
    except Exception as e:
        logger.error(e)
        raise
# extractInsertQuery : 데이터 INSERT 쿼리 추출
# martData : INSERT 대상 데이터
# tableName : INSERT 테이블 명
def extractInsertQuery(martData, tableName, logger):
    try:
        logger.info("데이터 INSERT 쿼리 추출")
        # 정제 후 값을 튜플화
        sql_rows = []
        for idx in martData.index:
            stringLi = []
            sql_row_pre = martData.iloc[idx].to_string(index=False, header=False).split('\n')
            # 1.value 앞뒤로 띄어쓰기 제거
            sql_row_pre2 = list(map(lambda y: y.strip(), sql_row_pre))
            # 2.value 값에 ",' 가 존재 할 경우 채워 넣음
            for i in sql_row_pre2:
                s = '"' + i.replace("'", "\\'").replace('"', '\\"') + '"'
                stringLi.append(s)
            # 3. 값 전체 튜플화
            sql_row = "(" + ','.join(stringLi) + ")"
            sql_rows.append(sql_row)
        # 컬럼 값의 경우 `column`으로 변경 후 튜플화
        col = []
        for dbCol in martData.columns:
            renameCol = "`" + dbCol + "`"
            col.append(renameCol)
        tableCol = "(" + ','.join(tuple(col)) + ")"
        # 쿼리문 작성
        sql = "INSERT IGNORE INTO " + tableName + " " + tableCol + " VALUES " + ','.join(sql_rows)
        logger.info("데이터 INSERT 쿼리 완료")
        return sql
    except Exception as e:
        logger.error(e)
        raise
