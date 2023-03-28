# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ##### Step1 - Spark Dataframe Reader를 사용해서 CSV파일 읽기

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/adbtesthudsonstorage/raw"))

# COMMAND ----------

# Header 를 식별할수 없음
# circuits_df = spark.read.csv("dbfs:/mnt/adbtesthudsonstorage/raw/circuits.csv")

# 첫 번째 행을 Header로 설정하기 위해 header 파라미터 값을 true로 전달
circuits_df = spark.read.csv("dbfs:/mnt/adbtesthudsonstorage/raw/circuits.csv", header=True)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

# 내부 데이터에 대한 정보 제공 1
# 스키마 출력
circuits_df.printSchema()

# 내부 데이터에 대한 정보 제공 2
circuits_df.describe().show()

# COMMAND ----------


