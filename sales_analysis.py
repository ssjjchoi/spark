from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, avg
import pandas as pd
import matplotlib.pyplot as plt

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .getOrCreate()

# 로그 깔끔하게
spark.sparkContext.setLogLevel("ERROR")

# CSV 읽기
df = spark.read.csv("sales.csv", header=True, inferSchema=True)

# 총 매출 컬럼 추가
df = df.withColumn("total_sales", col("price") * col("quantity"))

# 카테고리별 매출 합계
category_sales = df.groupBy("category").agg(sum_("total_sales").alias("category_total_sales"))

# 날짜별 평균 매출
daily_avg = df.groupBy("date").agg(avg("total_sales").alias("avg_sales"))

# Spark DataFrame → Pandas DataFrame 변환
category_pd = category_sales.toPandas()
daily_pd = daily_avg.toPandas()

# 시각화
plt.figure(figsize=(10,4))

# 카테고리별 매출
plt.subplot(1,2,1)
plt.bar(category_pd['category'], category_pd['category_total_sales'], color='skyblue')
plt.title("Category Total Sales")
plt.xlabel("Category")
plt.ylabel("Total Sales")

# 날짜별 평균 매출
plt.subplot(1,2,2)
plt.plot(daily_pd['date'], daily_pd['avg_sales'], marker='o', color='orange')
plt.title("Daily Average Sales")
plt.xlabel("Date")
plt.ylabel("Average Sales")
plt.xticks(rotation=45)

plt.tight_layout()
plt.show()

# Spark 세션 종료
spark.stop()
