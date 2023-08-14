DATE=`date '+%Y%m' -d '-1 month'`
echo "인허가 데이터를 통해 요식업종 데이터 마트 추출"

/usr/local/lib/spark/bin/spark-submit \
        --driver-memory 8g \
        --executor-memory 10g \
        --num-executors 5 \
        --conf spark.driver.maxResultSize=4g \
        --conf spark.debug.maxToStringFields=100 \
        --driver-java-options "-Dlog4j.configuration=log4j.properties" \
        --driver-class-path at.jar \
        --class make_mart.RunCateringMart\
        at.jar $DATE $DATE

echo "요식업종 데이터 마트 추출 완료"

source ~/.local/bin/at2/bin/activate
python ~/at2/run.py $DATE
