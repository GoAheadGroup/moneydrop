# Databricks notebook source
# MAGIC %md <h2>Create a Kimball-Hall star schema based on journey fact and time dimension</h2>

# COMMAND ----------

# MAGIC %md <h4>Load provider</h4>

# COMMAND ----------

 spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
 spark.conf.set("dfs.adls.oauth2.client.id", "d4f3a8b4-7404-4531-baea-c1a9c7c3e924")
 spark.conf.set("dfs.adls.oauth2.credential", "023b0f4c-0502-44e5-b892-55b8bdea3007")
 spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/7b9a801b-9908-4eba-a06d-218ad18224e3/oauth2/token")

# COMMAND ----------

# MAGIC %md <h4>Load in the Plymouth data</h4>

# COMMAND ----------

from pyspark.sql.types import *;

money = spark \
    .read \
    .option("header","true") \
    .format("csv") \
    .load('adl://moneydrop.azuredatalakestore.net/Ticketer/Plymouth Citybus.csv')    

# COMMAND ----------

# MAGIC %md <h6>Clean - remove the extra headers throughout the file</h6>

# COMMAND ----------

money = money.filter(money['IssuedAt'] != "IssuedAt")

# COMMAND ----------

# MAGIC %md <h6>Transform - Add OpCo</h6>

# COMMAND ----------

from pyspark.sql.functions import lit

money = money.withColumn("OpCode",lit("GSC"))

# COMMAND ----------

import pyspark
import math
import datetime
from pyspark.sql.functions import date_format
from pyspark.sql import SQLContext
from pyspark.sql.types import *

def retailmethod(paid,smartcard):
  if (paid=="Cash"):
      return("CASH")
  if (paid=="Card"):
      return("CONTACTLESS")
  if (paid=="Punch"):
      return("PUNCH")
  if (smartcard):
      try:
        smart = float(smartcard)
        return("SMARTCARD")
      except:
        return("M-TICKET")
      
  return("PAPER")
#fed

def pricefloat(strPrice):
   return float(strPrice)

# register the two user defined functions

retailDerivation_udf = udf(retailmethod, StringType())
float_udf = udf(pricefloat,FloatType())

money = money.withColumn("RetailMethod",retailDerivation_udf(money["Method"],money["Smartcard"])) \
             .withColumn("PriceNEW",float_udf(money["Price"])) \
             .drop("price") \
             .withColumnRenamed("PriceNew","Price")
#money = money.printSchema()
#money.show(10)

# COMMAND ----------

# MAGIC %md <h4>Convert the date to a datetimestamp</h4>

# COMMAND ----------

import pyspark
import datetime
from pyspark.sql.types import *

def convDateTime(strTime):
  try:
    newtime = datetime.datetime.strptime(strTime,"%d/%m/%y %H:%M:%S")
  except:
    return(none)
  return(newtime)
#fed

datetime_udf = udf(convDateTime,TimestampType())

money = money.withColumn("issuedDateTime", datetime_udf(money["IssuedAt"])) \
             .drop("IssuedAt") \
             .withColumnRenamed("IssuedDateTime","IssuedAt")

# COMMAND ----------

# MAGIC %md <h4>Prepare Journey Fact Table</h4>

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit
from pyspark.sql.functions import *

money = money.withColumnRenamed("From Stage","fromStage") \
             .withColumnRenamed("To Stage","toStage") \
             .withColumnRenamed("Journey Code","journeyCode") \
             .withColumnRenamed("Vehicle Registration","vehicleRegistration") \
             .withColumnRenamed("Bus Stop","busStop") \
             .withColumnRenamed("Bus Stop Atco","busStopAtco") \
             .withColumnRenamed("Ticket Type","ticketType") \
             .withColumn("time_id",concat( year(money['issuedAt']), \
                                               month(money['issuedAt']), \
                                               dayofmonth(money['issuedAt']), \
                                               hour(money['issuedAt']), \
                                               minute(money['issuedAt']) \
                                             ) \
                            ) \


# COMMAND ----------

# MAGIC %md <h4>Create Time Dimension</h4>

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import concat, col, lit

time = money.select("IssuedAt") \
             .withColumn("year",year(money['IssuedAt'])) \
             .withColumn("day",dayofmonth(money['IssuedAt'])) \
             .withColumn("month",month(money['IssuedAt'])) \
             .withColumn("hour",hour(money['IssuedAt'])) \
             .withColumn("minute",minute(money['IssuedAt'])) \
             .withColumn("dayofweek",date_format(money['IssuedAt'],'EEEE')) \
             .withColumn("weekofyear",weekofyear(money['IssuedAt'])) \
             .withColumn("time_id",concat(col("year"), col("month"), col("day"),col("hour"),col("minute"))) \
             .drop('IssuedAt') \
             .distinct()

# COMMAND ----------

# MAGIC %md <h4>Create a Product Dimension</h4>

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import concat, col, lit

product = money.select('ticketType','Price')

# COMMAND ----------

# MAGIC %md <h4>Final cleanup of keys</h4>

# COMMAND ----------

money = money \
      .drop('issuedAt') \
      .drop('Latitude') \
      .drop('Longtitude') \
      .drop('Driver')

# COMMAND ----------

# MAGIC %md <h6>Upload Fact and Dimensions </h6>

# COMMAND ----------

#money.printSchema()
#time.printSchema()

money.write.saveAsTable("fact_journey")
time.write.saveAsTable("dim_time")
