# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Databricks 에서 일반적으로 사용되는 Utilities 들 
# MAGIC #### File System Utilities
# MAGIC 노트북에서 Databricks 파일 시스템에 엑세스할 수 있으며, 다양한 파일 시스템 레벨?을 사용할 수 있습니다.
# MAGIC 
# MAGIC #### Secrets Utilities
# MAGIC Databricks에서 지원하는 secret scopes에 저장된 secret 혹은 Azure Key Vault의 값을 얻을 수 있습니다.
# MAGIC 
# MAGIC #### Widget Utilities
# MAGIC 호출 노트북이나 다른 애플리케이션(예: Azure Data Factory)이 실행 시 노트북에 매개 변수 값을 전달할 수 있도록 노트북을 매개 변수화할 수 있습니다.
# MAGIC 노트북을 재사용 가능하게 함으로써 유용합니다.
# MAGIC 
# MAGIC #### Notebook Workflow Utilities
# MAGIC 노트북 워크플로 유틸리티를 사용하면 다른 노트북에서 노트북을 호출하여 서로 연결할 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### File System Utilities
# MAGIC %fs를 통해 명령어 실행 시 databricks는 기본적으로 결과를 얻기 위해 dbutils.fs라는 dbutils 패키지를 호출합니다. </br>
# MAGIC 이를 python 코드로 직접 호출 가능합니다. </br>
# MAGIC python 코드로 실 dbutils 패키지 실행 시 다른 python, Scala, R 등의 패키지와 결합이 가능하므로 더 큰 유연성을 제공합니다. </br>

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets')

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

print(type(dbutils.fs.ls('/databricks-datasets/COVID')))

# 디렉토리만 출력하는 예시
for files in dbutils.fs.ls('/databricks-datasets/COVID'):
    if files.name.endswith('/'):
        print(files.name)


# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('ls')

# COMMAND ----------

# DBTITLE 1,스크립트
# MAGIC %md 
# MAGIC 
# MAGIC #### 스크립트
# MAGIC 
# MAGIC Databricks Utilities
# MAGIC 
# MAGIC make it easier to combine different types of tasks in a Single notebook.
# MAGIC 
# MAGIC For example, they allow us to combine file operations with ETL tasks.
# MAGIC 
# MAGIC These utilities can only be run from Python, Scala or R cells in a Notebook.
# MAGIC 
# MAGIC They cannot be run from a SQL cell.
# MAGIC 
# MAGIC Databricks have been coming up with a number of utilities recently.
# MAGIC 
# MAGIC Some are in preview and others are available for general availability.
# MAGIC 
# MAGIC Amongst those, the following are the most commonly used utilities.
# MAGIC 
# MAGIC We've already seen the File System Utilities in the last lesson.
# MAGIC 
# MAGIC It allows us to access databricks file system from a notebook and you can use various file system level
# MAGIC 
# MAGIC operations.
# MAGIC 
# MAGIC Secrets Utilities allow us to get secret values from secrets, which are stored in secret scopes backed by Databricks
# MAGIC 
# MAGIC or Azure Key Vault. Widget Utilities allows us to parameterized notebooks so that a calling notebook
# MAGIC 
# MAGIC or another application, for example, a Data Factory Pipeline can pass a parameter value to the notebook
# MAGIC 
# MAGIC at runtime.
# MAGIC 
# MAGIC This is really useful to make a notebook reusable. Notebook Workflow Utilities allow us to invoke one
# MAGIC 
# MAGIC notebook from another and chain them together.
# MAGIC 
# MAGIC In the next section of the course, we'll be using the File System Utilities to mount containers from
# MAGIC 
# MAGIC Azure Data Lake Storage into Databricks.
# MAGIC 
# MAGIC Also, we'll be using Secret Utilities to get the secrets from Azure Key Vault. As part of the Formula
# MAGIC 
# MAGIC One project development, we will use Widget Utilities to pass parameters into notebooks and Workflow
# MAGIC 
# MAGIC Utilities to chain notebooks together.
# MAGIC 
# MAGIC In this lesson, I'll show you how to use one of those utilities and access help, etc..
# MAGIC 
# MAGIC Let's switch over to the Databricks workspace and get started.
# MAGIC 
# MAGIC Ok, here we are back in the Databricks workspace.
# MAGIC 
# MAGIC I'm going to create a new Notebook.
# MAGIC 
# MAGIC And let's call this as databricks utilities.
# MAGIC 
# MAGIC And let's keep the default language as Python again and let's attach to the Cluster that we've got.
# MAGIC 
# MAGIC And let's click Create.
# MAGIC 
# MAGIC Let's start with the File System Utilities.
# MAGIC 
# MAGIC You may remember from the last lesson that we used the magic command %fs, to access the file
# MAGIC 
# MAGIC system like this.
# MAGIC 
# MAGIC That's basically listed the folders within the databricks root folder.
# MAGIC 
# MAGIC But if you wanted to see the contents of this folder for example, you would just do ls/databricks-datasets,
# MAGIC 
# MAGIC and that list the contents of that folder.
# MAGIC 
# MAGIC But when you run this command, databricks basically called the dbutils package called dbutils.fs
# MAGIC 
# MAGIC to get the results.
# MAGIC 
# MAGIC You can directly call the dbutils.fs package, instead to list these files.
# MAGIC 
# MAGIC Let's do that.
# MAGIC 
# MAGIC dbutils.fs package offers a number of methods to perform file system operations and ls is one
# MAGIC 
# MAGIC of them.
# MAGIC 
# MAGIC So let's access that method and we need to pass in the folder, from which we want a list of files or
# MAGIC 
# MAGIC folders in.
# MAGIC 
# MAGIC In order to access the root folder, you would do a / and that list all the folders and
# MAGIC 
# MAGIC files within the root folder.
# MAGIC 
# MAGIC As we saw previously, we only have these two folders. But if you wanted to see the contents of the
# MAGIC 
# MAGIC databricks-datasets folder, again you would pass in the folder name there and it will list all
# MAGIC 
# MAGIC the files and folders within that folder.
# MAGIC 
# MAGIC So that's what you're saying in a tabular format on the %fs ls and here when you access the
# MAGIC 
# MAGIC dbutils.fs.ls, you see the same contents, but you've got a list which is a Python list
# MAGIC 
# MAGIC as an output.
# MAGIC 
# MAGIC I mentioned this before, but this folder here databricks-datasets contains a number of publicly
# MAGIC 
# MAGIC available datasets, which you can use in your pet projects.
# MAGIC 
# MAGIC So as a Data Engineer you might want to develop your own projects and if you want to do that in Databricks,
# MAGIC 
# MAGIC this is a really good place for you to look at.
# MAGIC 
# MAGIC For example, if you want to see some Covid related data, you can just go into the COVID folder and
# MAGIC 
# MAGIC that's got a number of files that you can use in your projects.
# MAGIC 
# MAGIC As you can see, we've got the COVID folder here with a number of datasets, which includes hospital
# MAGIC 
# MAGIC beds and the infections and things like that about COVID.
# MAGIC 
# MAGIC So I would encourage you to look at this data if you are interested in doing any pet projects.
# MAGIC 
# MAGIC But coming back to talking about dbutils, you might be wondering why we would want to use the
# MAGIC 
# MAGIC dbutils package, instead of the Magic Command %fs.
# MAGIC 
# MAGIC The answer is dbutils package provides a greater flexibility, as it can be combined with other native
# MAGIC 
# MAGIC languages like Python, Scala or R.
# MAGIC 
# MAGIC For example, If I want to go through the list of files here and perform some additional operations, I simply have
# MAGIC 
# MAGIC to write a for loop to iterate through this list, and add any additional manipulations as I need.
# MAGIC 
# MAGIC So let's try one.
# MAGIC 
# MAGIC I'm going to just do a for loop here.
# MAGIC 
# MAGIC So in this for loop, I've just simply taken the output from the dbutils.fs.ls command and just
# MAGIC 
# MAGIC iterated through the occurrences and just printed them here.
# MAGIC 
# MAGIC But that's quite simple.
# MAGIC 
# MAGIC But let's make it a bit more meaningful.
# MAGIC 
# MAGIC Let's say I want to get only the subfolders within this folder and ignore any files.
# MAGIC 
# MAGIC For example, I do not want to see this file here,
# MAGIC 
# MAGIC .DS_Store,
# MAGIC 
# MAGIC and I wouldn't want to see the file here, which is download daily covid-19 datasets.sh.
# MAGIC 
# MAGIC But I want to see everything else because they are all folders.
# MAGIC 
# MAGIC So I can simply add an if statement and say wherever the name ends with the /, give me just those,
# MAGIC 
# MAGIC that'll ignore those two files.
# MAGIC 
# MAGIC Let's do that.
# MAGIC 
# MAGIC As you can see, that's only listed
# MAGIC 
# MAGIC the subfolders and the two files we talked about are not now missing in this output.
# MAGIC 
# MAGIC Even going further,
# MAGIC 
# MAGIC If I just wanted to see the names here and ignore every other attribute here, I can just do
# MAGIC 
# MAGIC files.name and I'll get the list of folder names here.
# MAGIC 
# MAGIC That's brilliant.
# MAGIC 
# MAGIC So as you can see, we've quite easily combined dbutils with Python to make it more powerful.
# MAGIC 
# MAGIC If you ask me when to use the %fs command and when to use the dbutils package, I tend to
# MAGIC 
# MAGIC use %fs magic command if I'm doing some ad-hoc queries, and I use the dbutils.fs package if
# MAGIC 
# MAGIC I'm doing something programmatically like this.
# MAGIC 
# MAGIC So far we've only been working with Python, but you can do the same with Scala and R, but you can't
# MAGIC 
# MAGIC do this with SQL.
# MAGIC 
# MAGIC Now that we understand the power of dbutils, let me show you how to get help on them. In order to get help,
# MAGIC 
# MAGIC you just
# MAGIC 
# MAGIC run the method Help
# MAGIC 
# MAGIC on the dbutils package.
# MAGIC 
# MAGIC Let's do that.
# MAGIC 
# MAGIC And as you can see, that's listed all the utilities available.
# MAGIC 
# MAGIC But please note that some are in experimental mode and some are in preview.
# MAGIC 
# MAGIC But let's say we want to find all the methods that are available in one of these utilities.
# MAGIC 
# MAGIC Let's do that for the File System Utility, because we are familiar with that one.
# MAGIC 
# MAGIC In order to do that, we just have to invoke the help method on the dbutils.fs package.
# MAGIC 
# MAGIC Let's do that.
# MAGIC 
# MAGIC As you can see, we've got a description about the dbutils.fs package and also the methods available
# MAGIC 
# MAGIC within that.
# MAGIC 
# MAGIC We've got some File System Utilities here and we've got some mount methods here, but we will be using
# MAGIC 
# MAGIC these mount methods in the next lesson or the next section of the course extensively.
# MAGIC 
# MAGIC So we'll talk about that when we get to that.
# MAGIC 
# MAGIC But in the meantime, let's say I want to get help on one of these methods.
# MAGIC 
# MAGIC In order to do that, again, you invoke the help method and then pass in the method in which you want
# MAGIC 
# MAGIC to get the help.
# MAGIC 
# MAGIC So I'm going to get help on the ls method,
# MAGIC 
# MAGIC so that will be dbutils.fs.help and then you pass in ls.
# MAGIC 
# MAGIC As you can see, that's given us the description and also the examples for us to look at.
# MAGIC 
# MAGIC So that's really useful.
# MAGIC 
# MAGIC I hope you've got a good understanding about how to use Help now. In this lesson,
# MAGIC 
# MAGIC I just wanted to show you how to use dbutils package and the power of it.
# MAGIC 
# MAGIC If you haven't understood all of it, please don't worry,
# MAGIC 
# MAGIC we'll be using these throughout the course. And by the end of the course I'm sure you will be familiar
# MAGIC 
# MAGIC using dbutils packages.
# MAGIC 
# MAGIC So that's the end of this lesson.
# MAGIC 
# MAGIC See you in the next one.
