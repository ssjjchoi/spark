from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, avg

# 1. Spark 세션 생성
spark = SparkSession.builder \
    .appName("Spark Toy Project") \
    .getOrCreate()

# 2. CSV 읽기
df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

# 3. 데이터 확인
df.show()

# 4. 총 매출 컬럼 추가
df = df.withColumn("total_sales", col("price") * col("quantity"))
df.show()

# 5. 카테고리별 매출 합계
category_sales = df.groupBy("category").agg(sum_("total_sales").alias("category_total_sales"))
category_sales.show()

# 6. 날짜별 평균 판매 금액
daily_avg = df.groupBy("date").agg(avg("total_sales").alias("avg_sales"))
daily_avg.show()