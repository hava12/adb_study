# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Databricks에서 데이터 저장소에 액세스하는 방법
# MAGIC 
# MAGIC Azure Databricks에서는 일반적으로 Azure Data Lake Storage Gen2를 Data Lake 스토리지 솔루션으로 사용합니다. <br>
# MAGIC 
# MAGIC 이번 강의에서는 Azure Data Lake Storage Gen2의 스토리지 데이터에 엑세스하는 방법을 살펴봅니다.
# MAGIC 
# MAGIC 1. Storage Access Keys 를 사용한 Access
# MAGIC 1. Shared Access Signature를 사용한 Access (SAS Token)
# MAGIC 1. Service Principal을 사용한 Access
# MAGIC 
# MAGIC 추가적인 인증 방식으로는 
# MAGIC AAD Paththrough 인증 또는 Azure Active Directory 라고 불리는 인증이 있습니다.
# MAGIC 
# MAGIC 1. Create Azure Data Lake Gen2 Storage
# MAGIC 1. Access Data Lake using Access Keys
# MAGIC 1. Access Data Lake using SAS Token
# MAGIC 1. Access Data Lake using Service Principal
# MAGIC 1. Using Cluster Scoped Authentication
# MAGIC 1. Access Data Lake using AAD Credential Pass-through

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Access Keys를 이용한 Azure Data Lake Gen 2 Access
# MAGIC Azure는 Storage 계정을 만들 때 두 개의 계정 액세스 키를 생성합니다. <br>
# MAGIC 액세스 키가 있으면 소유자가 할 수 있는 모든 작업을 수행 가능합니다. <br>
# MAGIC 
# MAGIC Azure Key Vault를 사용하여 보안을 유지합니다.
# MAGIC 
# MAGIC Access Key를 <b>fs.azure.account.key</b>라는 Spark 구성에 할당하여 수행할 수 있습니다.
# MAGIC 
# MAGIC spark.conf.set("fs.azure.account.key.<Storage-account>.dfs.core.windows.net", "<access-key>")
# MAGIC 
# MAGIC Storage 계정에 저장된 데이터에 액세스하려면 <b>ABFS</b> 또는 <b>Azure Blob 파일 시스템 드라이버</b>를 사용하는 것이 좋습니다. 
# MAGIC   
# MAGIC abfs 예시 - abfs[s]://<container>@<storage_account_name>.dfs.core.windows.net/<folter_path>/<file_name>
# MAGIC <br>
# MAGIC <br>
# MAGIC - 사용자에게 전체 권한을 주기 때문에 문제가 될 수 있음.
# MAGIC   

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adbtesthudsonstorage.dfs.core.windows.net", "<access-key>")
display(dbutils.fs.ls("abfss://demo@adbtesthudsonstorage.dfs.core.windows.net"))



# COMMAND ----------

df = spark.read.csv('abfss://demo@adbtesthudsonstorage.dfs.core.windows.net/circuits.csv', )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 위의 소스는 Access Key 문자열이 그대로 노출되기 때문에 보안에 취약합니다.
# MAGIC 
# MAGIC 따라서 Key vault를 이용한 암호화가 필요합니다.
# MAGIC 
# MAGIC 다음 섹션인 Securing Access to Azure Data Lake 에서 해당 방법에 대해 알아봅니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Shared Access Signature (SAS Token)를 이용한 Azure Data Lake Gen 2 Access
# MAGIC 
# MAGIC 액세스키와 달리 보다 세분화된 수준에서의 액세스 제어가 가능합니다. (권장)
# MAGIC 
# MAGIC ex) 쓰기 제외 읽기 권한만 부여 등
# MAGIC 
# MAGIC 1. 인증 유형을 SAS 또는 Shared Access Signature로 정의 (spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS"))
# MAGIC 1. SAS 토큰 공급자를 고정 SAS토큰 공급자로 정의 (spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"))
# MAGIC 1. SAS 토큰의 값을 설정 (spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", "<token>"))

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adbtesthudsonstorage.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adbtesthudsonstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adbtesthudsonstorage.dfs.core.windows.net", "<token>")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@adbtesthudsonstorage.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.csv('abfss://demo@adbtesthudsonstorage.dfs.core.windows.net/circuits.csv', )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Service Principal을 이용한 Azure Data Lake Gen 2 Access
# MAGIC 
# MAGIC Azure 구독에서 리소스에 접근할 수 있는 권한을 갖기 위해 Azure Active Directory에 등록된 사용자나 그룹에게 RBAC를 이용하여 권한을 부여할 수 있습니다. 
# MAGIC 
# MAGIC 이를 통해 Azure 구독에서 리소스에 대한 액세스를 효율적으로 관리할 수 있습니다.
# MAGIC 
# MAGIC 
# MAGIC 1. Service Principal 등록 (Service Principal은 Azure AD 애플리케이션 또는 Active Directory 애플리케이션이라고도 합니다.)
# MAGIC 1. Service Principal에 대한 secret 생성
# MAGIC 1. Service Principal을 통해 스토리지 계정에 액세스하도록 Databricks 구성 (Spark 구성 파라미터를 통해 가능)
# MAGIC 1. Service Principal에 대해 데이터 레이크에 역할 할당

# COMMAND ----------

client_id = ""
tanent_id = ""

service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Cluster 범위의 인증
# MAGIC 
# MAGIC 기존 세션 단위의 인증이 아닌 클러스터 단위의 인증 <br>
# MAGIC 모든 클러스터 사용자에게 동일한 권한을 주는 경우 유용하며, 대규모 프로젝트에서는 부적절할 수 있습니다. <br>
# MAGIC <br>
# MAGIC 
# MAGIC 1. Compute -> 설정하려는 클러스터 선택
# MAGIC 1. Advanced options 선택
# MAGIC 1. spark config 에 설정 내용 넣기
# MAGIC 
# MAGIC 클러스터 설정 내부에 secret을 하드코딩하는 것은 좋은 방법은 아닙니다.<br>
# MAGIC 추후 key-Vault를 사용하는 방법으로 수정이 필요합니다.
