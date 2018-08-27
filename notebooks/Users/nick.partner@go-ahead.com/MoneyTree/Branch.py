# Databricks notebook source
# MAGIC %md <h2>Setup the data sources</h2>

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

# MAGIC %md <h4>Remove the extra headers throughout the file as the data is a little dirty so we can manipulate later</h4>

# COMMAND ----------

money = money.filter(money['IssuedAt'] != "IssuedAt")

# COMMAND ----------

money.count()

# COMMAND ----------

# MAGIC %md We want to:
# MAGIC 
# MAGIC   1. create a new field which says what type of ticket the customer is using: CASH, CONTACTLESS, M-TICKET or SMARTCARD. 
# MAGIC   2. sort out a new date only field which is a string so we can aggregate by dates

# COMMAND ----------

import pyspark
import math
import datetime
from pyspark.sql.functions  import date_format
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, countDistinct

def retailmethod(paid,smartcard):
  if (paid=="Cash"):
      return("CASH")
  if (paid=="Card"):
      return("CONTACTLESS")
  if (smartcard):
      try:
        smart = float(smartcard)
        return("SMARTCARD")
      except:
        return("M-TICKET")
      
  return("PAPER")
#fed

def dateonly(strDate):

  # 01/04/18 00:00:30
  try: 
    newdate = datetime.datetime.strptime(strDate,"%d/%m/%y %H:%M:%S")
    return(newdate.date())
  except:
    newdate = datetime.datetime.strptime(strDate,"%d/%m/%y %H:%M:%S")
    return(newdate.date())
    #return(datetime.datetime.now())

#fed

# register the two user defined functions

retailDerivation_udf = udf(retailmethod, StringType())
datetime_udf = udf(dateonly,DateType())

df = money.withColumn("retailmethod", retailDerivation_udf(money['method'],money['smartcard']))
df = df.withColumn("issuedDate", datetime_udf(df["IssuedAt"]))


# COMMAND ----------

# MAGIC %md <h4>Register as table</h4>

# COMMAND ----------

from pyspark.sql import SQLContext, Row
sqlContext2 = SQLContext(spark)

sqlContext2.registerDataFrameAsTable(df, "money")

# COMMAND ----------

# MAGIC %md <h2>Now we can do something useful!</h2>

# COMMAND ----------

# MAGIC %md <h4>Which services are the busiest?</h4>

# COMMAND ----------


display(sqlContext.sql("select service,count(Service) as serviceuse from money group by Service order by serviceuse "))


# COMMAND ----------

# MAGIC %md <h4>What is the split of ticket type by passengers?</h4>

# COMMAND ----------

display(sqlContext.sql("select retailmethod as ticketusage, count(retailmethod) from money group by ticketusage"))

# COMMAND ----------

# MAGIC %md <h4>How many contactless journeys were made?</h4>

# COMMAND ----------

display(sqlContext.sql("select count(issueddate) as numberjourneys, retailmethod, issueddate from money group by issueddate,retailmethod order by issueddate asc"))

# COMMAND ----------

display(sqlContext.sql("select count(issueddate) as numberjourneys,issueddate from money where retailmethod='CONTACTLESS' group by issueddate order by issueddate asc"))

# COMMAND ----------

# MAGIC %md <h4>So what about total passenger journeys?</h4>

# COMMAND ----------

display(sqlContext.sql("select count(issueddate) as numberjourneys,issueddate from money where group by issueddate order by issueddate asc"))