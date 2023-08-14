### DB 접속 정보 코드 ###

import pymysql
from clickhouse_driver import connect
# atLoadSaveDBProp : at 관련 데이터 접속 정보 추출
def atLoadSaveDBProp(logger):
    try:
        logger.info("개발 DB 접속")
        host = ""
        user = ""
        password = ""
        db = ""
        charset = "utf8"
        conn = pymysql.connect(host=host, user=user, password=password,
                               db=db, charset=charset, cursorclass=pymysql.cursors.DictCursor)
        logger.info("개발 DB 접속완료")
        return conn
    except Exception as e:
        logger.error(e)
        raise
# impDBProp : 수출입 데이터 접속 정보 추출
def impDBProp(logger):
    try:
        logger.info("수출입 DB 접속")
        host = ""
        port = ""
        database = ""
        user = ""
        password = ""

        conn = connect(host=host,
                       port=port,
                       database=database,
                       user=user,
                       password=password)
        logger.info("수출입 DB 접속완료")
        return conn
    except Exception as e:
        logger.error(e)
        raise
# atFinalSaveDBProp : at 결과 저장 테이블 관련 정보
def atFinalSaveDBProp(logger):
    try:
        logger.info("결과테이블 DB 접속")
        host = ""
        user = ""
        password = ""
        db = ""
        charset = "utf8"
        conn = pymysql.connect(host=host, user=user, password=password,
                               db=db, charset=charset, cursorclass=pymysql.cursors.DictCursor)
        logger.info("결과테이블 DB 접속완료")
        return conn
    except Exception as e:
        logger.error(e)
        raise
