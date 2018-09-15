# Databricks notebook source
# MAGIC %md <h2>Create a Kimball-Hall star schema based on money fact with ACT data</h2>

# COMMAND ----------

# MAGIC %md <h6>Load provider</h6>

# COMMAND ----------

 spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
 spark.conf.set("dfs.adls.oauth2.client.id", "d4f3a8b4-7404-4531-baea-c1a9c7c3e924")
 spark.conf.set("dfs.adls.oauth2.credential", "023b0f4c-0502-44e5-b892-55b8bdea3007")
 spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/7b9a801b-9908-4eba-a06d-218ad18224e3/oauth2/token")

# COMMAND ----------

# MAGIC %md <h6>Generate a list of files to load</h6>

# COMMAND ----------

# MAGIC %md <h6>Load in the files</h6>

# COMMAND ----------

act = spark.read.option('header','true').format('csv').load("adl://moneydrop.azuredatalakestore.net/ACT/PLY/*topup*")
retailpost = spark.read.option('header','true').format('csv').load("adl://moneydrop.azuredatalakestore.net/ACT/PLY/*rp_report*")

# COMMAND ----------

# MAGIC %md <h6>Set the correct Operating Company code</h6>

# COMMAND ----------

from pyspark.sql.functions import lit

act = act.withColumn("OpCode",lit("PCB"))
retailpost = retailpost.withColumn("OpCode",lit("PCB"))


# COMMAND ----------

# MAGIC %md to do
# MAGIC 
# MAGIC * convert price /100 to pounds and pence
# MAGIC * convert date time and link to timeid
# MAGIC * map back to common_schema v1

# COMMAND ----------

# MAGIC %md <h6> convert to pounds and pence </h6>

# COMMAND ----------

import pyspark
import math
from pyspark.sql.types import *

def pricefloat(strPrice):
   flPrice = float(strPrice)
   if flPrice:
      return flPrice
   else: 
      return 0

float_udf = udf(pricefloat,FloatType())

act = act \
        .withColumn("Price",float_udf(act["Sale value"])) \
        .drop("Sale value")

retailpost = retailpost \
        .withColumn("NewPrice",float_udf(retailpost["Price"])) \
        .drop("Price") \
        .withColumnRenamed("NewPrice","Price")

# COMMAND ----------

act.show(10)

# COMMAND ----------

# MAGIC %md <h6>Filter out Removals</h6>

# COMMAND ----------

retailpost = retailpost.filter(retailpost['Transaction Type'] != 'REMOVAL')

# COMMAND ----------

# MAGIC %md <h6>Convert the date to a datetimestamp</h6>

# COMMAND ----------

import pyspark
import datetime
from pyspark.sql.types import *

# 01-JAN-2018 09:10:17

def convDateTime(strTime):
  try:
    newtime = datetime.datetime.strptime(strTime,"%d-%b-%Y %H:%M:%S")
  except:
    return(none)
  return(newtime)
#fed

def convDateTime2(strTime):
  try:
    newtime = datetime.datetime.strptime(strTime,"%Y/%m/%d %H:%M:%S")
  except:
    return(datetime.today())
  return(newtime)
#fed

datetime_udf = udf(convDateTime,TimestampType())
datetime2_udf = udf(convDateTime2,TimestampType())

act = act.withColumn("issuedDateTime", datetime_udf(act["Date time"]))
retailpost = retailpost.withColumn("issuedDateTime",datetime2_udf(retailpost["Transaction time"]))

# COMMAND ----------

# MAGIC %md <h6> generate the time_id parameter </h6>

# COMMAND ----------

from pyspark.sql.functions import *
import datetime

act = act.withColumn("time_id",concat( year(act['issuedDateTime']), \
                                               month(act['issuedDateTime']), \
                                               dayofmonth(act['issuedDateTime']), \
                                               hour(act['issuedDateTime']), \
                                               minute(act['issuedDateTime']) \
                                    ))

retailpost = retailpost.withColumn("time_id",concat( year(retailpost['issuedDateTime']), \
                                               month(retailpost['issuedDateTime']), \
                                               dayofmonth(retailpost['issuedDateTime']), \
                                               hour(retailpost['issuedDateTime']), \
                                               minute(retailpost['issuedDateTime']) \
                                    ))


# COMMAND ----------

# MAGIC %md <h6> add in additional fields </h6>

# COMMAND ----------

#salesreference

# COMMAND ----------

act = act \
        .withColumn("paymentMethod", lit("Card")) \
        .withColumn("saleLocation", lit('')) \
        .withColumnRenamed("Retail reference","productId") \
        .withColumnRenamed("Channel","retailChannel") \
        .withColumnRenamed("User","salesAgent") \
        .withColumnRenamed("Sales reference","salesReference") \
        .drop("Number of trips") \
        .drop("Date time") \
        .drop("Product reference")


# COMMAND ----------

retailpost = retailpost \
              .withColumnRenamed("Transaction type","transactionType") \
              .withColumnRenamed("Operator","salesAgent") \
              .withColumnRenamed("Product","productId") \
              .withColumn("retailChannel", lit("SHOP")) \
              .withColumn("paymentMethod", lit("unknown")) \
              .withColumn("salesReference",lit("")) \
              .drop("ISRN") \
              .drop("EPOS") \
              .drop("Supervisor") \
              .withColumnRenamed("site","saleLocation") \
              .drop("Passes") \
              .drop("Duration") \
              .drop("transactionType") \
              .drop("Value") \
              .drop("Transaction Time")

# COMMAND ----------

act = act.select("issuedDateTime","time_id","salesReference","salesAgent","saleLocation","productId","retailChannel","Price","paymentMethod","OpCode")
retailpost = retailpost.select("issuedDateTime","time_id","salesReference","salesAgent","saleLocation","productId","retailChannel","Price","paymentMethod","OpCode")
allAct = act.unionAll(retailpost)


# COMMAND ----------

allAct.show()

# COMMAND ----------

# MAGIC %md <h1> Queries below this line </h1>

# COMMAND ----------

from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(spark)

#sqlContext.registerDataFrameAsTable(allAct, "all")
sqlContext.registerDataFrameAsTable(act, "act")


# COMMAND ----------

act.show(5)

# COMMAND ----------

display(sqlContext.sql("select count(`Product reference`), ProductId, month(issuedDateTime) as pMonth from act group by ProductId, pMonth"))

# COMMAND ----------

allAct.count()

# COMMAND ----------

allAct.printSchema()

# COMMAND ----------

display(sqlContext.sql("select month(issuedDateTime) as pMonth,sum(Price) as revenue,productId from all where group by pMonth, productId order by pMonth asc"))

# COMMAND ----------

display(sqlContext.sql("select count(salesReference), month(issuedDateTime) as retailmonth, retailChannel from all group by retailmonth, retailChannel order by retailmonth asc"))

# COMMAND ----------

#display(sqlContext.sql("select sum(price) as revenue, month(issuedDateTime) as retailmonth from act group by retailmonth order by retailmonth asc"))

# COMMAND ----------

allAct.printSchema()

# COMMAND ----------

