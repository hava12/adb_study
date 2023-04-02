# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ##### Step1 - Spark Dataframe Reader를 사용해서 CSV파일 읽기

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/adbtesthudsonstorage/raw"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
                                        StructField("circuitId", IntegerType(), False),
                                        StructField("circuitRef", StringType(), True),
                                        StructField("name", StringType(), True),
                                        StructField("location", StringType(), True),
                                        StructField("country", StringType(), True),
                                        StructField("lat", DoubleType(), True),
                                        StructField("lng", DoubleType(), True),
                                        StructField("alt", IntegerType(), True),
                                        StructField("url", StringType(), True),
                                    ])

# COMMAND ----------

# Header 를 식별할수 없음
# circuits_df = spark.read.csv("dbfs:/mnt/adbtesthudsonstorage/raw/circuits.csv")

# 첫 번째 행을 Header로 설정하기 위해 header 파라미터 값을 true로 전달
circuits_df = spark.read.csv("dbfs:/mnt/adbtesthudsonstorage/raw/circuits.csv", \
                             header=True, \
                             inferSchema=True)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

# 내부 데이터에 대한 정보 제공 1
# 스키마 출력
circuits_df.printSchema()

# 내부 데이터에 대한 정보 제공 2
circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 필요한 열만 선택하여 조회

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

## 첫번째 방법 
##circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

## 두번째 방법
# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

## 세번째 방법
#circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

## 네번째 방법
# circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name").alias("name1"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))


# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 컬럼명 재정의

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuirId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")


# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 새 열을 추가하는 방법

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("imgestion_date", current_timestamp()) \
    .withColumn("env", lit("Production"))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DataLake에 Parquet로 저장하기

# COMMAND ----------

circuits_final_df.write.parquet("/mnt/adbtesthudsonstorage/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/adbtesthudsonstorage/processed/circuits

# COMMAND ----------


