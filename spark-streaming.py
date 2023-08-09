# Importing Libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

# Starting Spark Session

spark = SparkSession \
        .builder \
        .appName("RDA") \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Reading Input from kafka Producer
        
RawData = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","18.211.252.152:9092") \
        .option("startingOffsets", "latest") \
        .option("subscribe","real-time-project") \
        .load()

# Defining the Schema

JsonSchema = StructType() \
        .add("invoice_no", LongType()) \
        .add("country", StringType()) \
        .add("timestamp", TimestampType()) \
        .add("type", StringType()) \
        .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", DoubleType()),
        StructField("quantity", IntegerType())
   ])))

# Creating dataframe from input data after applying the schema 

OrderStream = RawData.select(from_json(col("value").cast("string"), JsonSchema).alias("data")).select("data.*")


# UDF for calculating total_items

def items_TotalCount(items):
   total_count = 0
   for item in items:
       total_count = total_count + item['quantity']
   return total_count

# UDF for calculating order type 

def is_order(type):
   if type=="ORDER":
       return 1
   else:
       return 0

# UDF for calculating return type
    
def is_return(type):
   if type=="RETURN":
       return 1
   else:
       return 0
     
    
# UDF for calculating total_cost

def TotalCostSum(items,type):
   total_sum = 0
   for item in items:
       total_sum = total_sum + item['unit_price'] * item['quantity']
   if type=="RETURN":
       return total_sum * (-1)
   else:
       return total_sum

    
# Converting to UDF’s with the utility functions

isorder = udf(is_order, IntegerType())
isreturn = udf(is_return, IntegerType())
totalcount = udf(items_TotalCount, IntegerType())
totalcost = udf(TotalCostSum, DoubleType())


# Calculating columns(total_cost, total_items, is_order, is_return) 

order_stream = OrderStream \
        .withColumn("total_cost", totalcost(OrderStream.items, OrderStream.type)) \
        .withColumn("total_items", totalcount(OrderStream.items)) \
        .withColumn("is_order", isorder(OrderStream.type)) \
        .withColumn("is_return", isreturn(OrderStream.type)) 

# Writing the Inetermediary data into Console

orderStreamOutput = order_stream \
       .select("invoice_no", "country", "timestamp","total_cost","total_items","is_order","is_return") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()

# Calculating time-based KPI

timeBasedKPIs = order_stream \
    .withWatermark("timestamp", "1 minute") \
    .groupby(window("timestamp", "1 minute", "1 minute")) \
    .agg(count("invoice_no").alias("OPM"),
         sum("total_cost").alias("total_sales_volume"), 
         avg("total_cost").alias("average_transaction_size"), 
         avg("is_return").alias("rate_of_return")) \
    .select("window", "OPM", "total_sales_volume", "average_transaction_size", "rate_of_return")

# Stream for time based KPIs

timeBasedKPIsOutput = timeBasedKPIs \
    .writeStream \
    .outputMode("Append") \
    .format("json") \
    .option("format","append") \
    .option("truncate", "false") \
    .option("path", "time-wise-kpi") \
    .option("checkpointLocation", "time-kpi") \
    .option("truncate", "False") \
    .trigger(processingTime="1 minute") \
    .start()

# Calculating time-based and country-based KPIs 

timeAndCountryBasedKPIs = order_stream \
    .withWatermark("timestamp", "1 minute") \
    .groupby(window("timestamp", "1 minute", "1 minute"), "country") \
    .agg(count("invoice_no").alias("OPM"),
         sum("total_cost").alias("total_sales_volume"), 
         avg("is_return").alias("rate_of_return")) \
    .select("window", "country", "OPM", "total_sales_volume", "rate_of_return") 
   

# Stream for time and country based KPIs 

timeAndCountryBasedKPIsOutput = timeAndCountryBasedKPIs \
    .writeStream \
    .outputMode("Append") \
    .format("json") \
    .option("format","append") \
    .option("truncate", "false") \
    .option("path", "time-country-wise-kpi") \
    .option("checkpointLocation","time-country-kpi") \
    .trigger(processingTime="1 minute") \
    .start()

# Waiting infinitely to read the data 

timeAndCountryBasedKPIsOutput.awaitTermination() 

