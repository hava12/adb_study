# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Securing Secrets
# MAGIC 
# MAGIC Databricks는 Secret Scope라는 서비스를 제공하고 Azure는 Key Vault라는 서비스를 제공합니다.
# MAGIC 
# MAGIC 두 가지를 결합하여 Azure Databricks에서 안전한 솔루션을 만들 수 있습니다.
# MAGIC 
# MAGIC ##### Secret Scopes
# MAGIC Secret Scope는 Databricks에서 소유하고 관리하는 암호화된 Databricks 데이터베이스에서 지원합니다. <br>
# MAGIC Databricks CLI 또는 API를 사용하여 암호화된 데이터베이스를 만들고 변경할 수 있습니다.
# MAGIC 
# MAGIC ##### Azure Key Vault
# MAGIC Azure 에서 Databricks를 사용할 때 권장하는 방식 <br>
# MAGIC 
# MAGIC 
# MAGIC ##### 순서
# MAGIC 1. Azure Key Vault를 만든 후 키 자격 증명 모음에 모든 Secrets을 추가
# MAGIC 1. Databricks Secret Scope를 생성 후 Azure Key Vault에 연결
# MAGIC 1. 작성 시 dbutils.secrets라는 Databricks Secret Utility를 사용하여 secret을 가져옵니다.
# MAGIC 
# MAGIC Azure Portal에서 Secret을 생성한 후, 
# MAGIC <databricks URL>/secrets/createScope 페이지로 이동합니다.
# MAGIC   
# MAGIC Scope Name을 지정 후
# MAGIC DNS Name과 
# MAGIC Resource ID를 입력하면 됩니다.
# MAGIC   
# MAGIC DNS Name과 Resource ID는 Azure Portal에서 key-vault 리소스로 이동 후 좌측 Properties메뉴로 이동하면 확인 가능합니다.

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("adbtesthudsonstorage-scope")

# COMMAND ----------

## 값을 가져오지만 노출하지 않습니다.
dbutils.secrets.get(scope='adbtesthudsonstorage-scope', key='adbtesthudsonstorage-key')

# COMMAND ----------


adbtesthudsonstoragekey = dbutils.secrets.get(scope='adbtesthudsonstorage-scope', key='adbtesthudsonstorage-key')

spark.conf.set("fs.azure.account.key.adbtesthudsonstorage.dfs.core.windows.net", adbtesthudsonstoragekey)
display(dbutils.fs.ls("abfss://demo@adbtesthudsonstorage.dfs.core.windows.net"))

# COMMAND ----------

sasToken = dbutils.secrets.get(scope='adbtesthudsonstorage-scope', key='adbtesthudsonstorage-demo-sastoken')

spark.conf.set("fs.azure.account.auth.type.adbtesthudsonstorage.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adbtesthudsonstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adbtesthudsonstorage.dfs.core.windows.net", sasToken)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@adbtesthudsonstorage.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 클러스터 범위에서 구성
# MAGIC 
# MAGIC 1. Compute - 클러스터 선택
# MAGIC 1. Advanced options 펼치기
# MAGIC 1. Spark config 로 이동
# MAGIC 1. 계정 키를 갖도록 하나의 구성 추가 (fs.azure.account.key.adbtesthudsonstorage.dfs.core.windows.net {{secrets/[scope]/[key]}})
