// Databricks notebook source
Class.forName("org.mariadb.jdbc.Driver")

// COMMAND ----------

val jdbcHostname = "poc-gcp.mysql.database.azure.com"
val jdbcPort = 3306
val jdbcDatabase = "excel"
val jdbcUsername = "svc_worker_poc_gcp"
val jdbcPassword = "66oJTIq4xZ#x"



// COMMAND ----------

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
//?useSSL=true&requireSSL=true
//ssl':{'fake_flag_to_enable_tls': True

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")


// COMMAND ----------

val df = spark.read.jdbc(jdbcUrl, "condicoes_pagamento", connectionProperties).toDF

// COMMAND ----------

df.write
  .format("bigquery")
  .mode("overwrite")
  .option("temporaryGcsBucket", "databricksmatrix")
  .option("table", "Bronze.condicoes_pagamento")
  .save()
