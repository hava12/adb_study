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

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### DBFS 마운트의 이점
# MAGIC 
# MAGIC 1. Credential 없이 Data에 Access할 수 있다.
# MAGIC 1. file semantics을 이용하여 긴 URL을 매번 작성할 필요가 없다.
# MAGIC 1. Azure Storage의 모든 이점을 얻을 수 있다. (DBFS는 추상화 계층일 뿐이다.)
# MAGIC 
# MAGIC ##### Azure Data Lake에 엑세스하기 위한 권장 솔루션 - Unity Catalog
# MAGIC 2022년 말에 GA
# MAGIC 
# MAGIC 그러나 현재까지는 마운트 방식을 많이 사용하고 있기 때문에 알고 있어야 함
# MAGIC 
# MAGIC Mount를 생성하기 위해서는 Service Principal을 생성해야 합니다.
# MAGIC 
# MAGIC #### Service Principal을 사용하여 Azure Data Lake에 Mount
# MAGIC 
# MAGIC 
# MAGIC <b>Step</b>
# MAGIC 1. key vault로부터 client_id, tanent_id, client_secret을 얻어옵니다.
# MAGIC 1. spark config를 세팅합니다.
# MAGIC 1. Storage에 마운트하기 위해 file system utilty를 호출합니다. 
# MAGIC 1. 마운트와 관련된 다른 파일 시스템 유틸리티 탐색
# MAGIC 
# MAGIC https://learn.microsoft.com/ko-kr/azure/databricks/dbfs/mounts

# COMMAND ----------

dbutils.secrets.help()

dbutils.secrets.listScopes()

dbutils.secrets.list('adbtesthudsonstorage-scope')

# COMMAND ----------


client_id = dbutils.secrets.get(scope="adbtesthudsonstorage-scope", key="adb-test-hudson-databricks-clientid")
tenant_id = dbutils.secrets.get(scope="adbtesthudsonstorage-scope", key="adb-test-hudson-databricks-tenantid")
client_secret = dbutils.secrets.get(scope="adbtesthudsonstorage-scope", key="adb-test-hudson-databricks-client-secret")

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}


# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@adbtesthudsonstorage.dfs.core.windows.net",
  mount_point = "/mnt/adbtesthudsonstorage/demo",
  extra_configs = configs
)

# COMMAND ----------

def mount(storage_account_name, container_name):

    client_id = dbutils.secrets.get(scope="adbtesthudsonstorage-scope", key="adb-test-hudson-databricks-clientid")
    tenant_id = dbutils.secrets.get(scope="adbtesthudsonstorage-scope", key="adb-test-hudson-databricks-tenantid")
    client_secret = dbutils.secrets.get(scope="adbtesthudsonstorage-scope", key="adb-test-hudson-databricks-client-secret")

    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs
    )
    display(dbutils.fs.mounts())

# COMMAND ----------

mount('adbtesthudsonstorage', 'demo');
mount('adbtesthudsonstorage', 'presentation');
mount('adbtesthudsonstorage', 'processed');
mount('adbtesthudsonstorage', 'raw');


# COMMAND ----------

display(dbutils.fs.ls("/mnt/adbtesthudsonstorage/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/adbtesthudsonstorage/demo")
dbutils.fs.unmount("/mnt/adbtesthudsonstorage/raw")
dbutils.fs.unmount("/mnt/adbtesthudsonstorage/presentation")
dbutils.fs.unmount("/mnt/adbtesthudsonstorage/processed")
