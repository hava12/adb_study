# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### 해당 섹션에서는
# MAGIC 1. Databricks File System (DBFS)가 무엇인지
# MAGIC 1. Databricks mounts가 무엇인지
# MAGIC 1. Databricks에 DataLakeStorage컨테이너를 Mount <br>
# MAGIC 
# MAGIC 을 수행합니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Databricks File System (DBFS)이란? 
# MAGIC 기본 Azure Blob Storage를 만들고 이를 DBFS에 탑재합니다. <br>
# MAGIC DBFS는 Databricks작업 영역에서 Azure Blob Storage와 상호작용하는 Databricks 파일 시스템 유틸리티입니다. <br>
# MAGIC DBFS자체로는 스토리지가 아니며 추상화된 계층일 뿐입니다. <br><br>
# MAGIC 
# MAGIC Azure Blob Storage의 DBFS마운트를 DBFS루트하고 합니다.
# MAGIC 
# MAGIC Managed Table의 기본 위치는 DBFS루트입니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### DBFS Root 살펴보기
# MAGIC 1. DBFS root 내의 모든 폴더 리스트
# MAGIC 1. DBFS File Browser와의 상호 작용
# MAGIC 1. DBFS Root에 파일 업로드

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Admin Console에서 DBFS 파일 브라우저 활성화
# MAGIC Admin Settings - Workspace settings으로 이동해 
# MAGIC DBFS File Browser: Disabled 를 Enabled로 변경

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))
