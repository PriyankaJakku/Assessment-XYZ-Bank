# Notebook sourcecode
import pyspark
from pyspark.sql import SparkSession,DataFrame,functions 
from pyspark.conf import SparkConf
from pyspark.sql.functions import col,trim,when,sum as _sum
from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType,DecimalType
from pyspark.sql.window import Window
import sys, os

# Create a SparkConf object
conf = SparkConf().setAppName("Assignemnt")
# Create a SparkSession object
spark = SparkSession.builder.config(conf=conf).getOrCreate()
Input_path = output_path = f"file:{os.getcwd()}/"

#Create Schema for Input File
Input_schema = StructType(
    [
        StructField("TransactionDate",DateType(),True),
        StructField("AccountNumber",IntegerType(),True),
        StructField("AccountType",StringType(),True),
        StructField("Amount",IntegerType(),True),
    ]
)

#Read input File
in_df = spark.read.schema(Input_schema).option("header","true").option("delimiter",";").option("dateFormat", "yyyyMMdd").csv(Input_path+"AccountTransaction.csv")
#Remove/ Truncate inbetween spaces for Column 
in_df = in_df.withColumn("AccountType",trim(col("AccountType")))

#Desired Output Implementation
df=in_df.withColumn("TransAmount",when(in_df.AccountType=="Debit",in_df.Amount * -1).otherwise(in_df.Amount))
windowspec = Window.partitionBy(["AccountNumber"]).orderBy("TransactionDate")
df=df.withColumn("CurrentBalance",_sum("TransAmount").over(windowspec))
df=df.drop("TransAmount")
#df.show()
df.write.csv(output_path+'Balance_AccountTransactions.csv')
