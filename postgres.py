import findspark
findspark.init()

import os
import shutil
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import logging
import sys
 
# total arguments


#n = len(sys.argv)
#print("Total arguments passed:", n)
#if n != 2:
#    sys.exit("Enter one argument - work directory(with data):")

logger = logging.getLogger('logger')

fh = logging.FileHandler('mylog.log')
fh.setLevel(logging.INFO)
logger.addHandler(fh)

arg = "/Users/simon/Downloads/задача_кандидата" #sys.argv[1]
rootPath = arg + "/"
csvPath = rootPath + "res.csv"
csvPath2 = rootPath + "res2.csv"


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Spark") \
    .config("spark.jars", rootPath+"postgresql-42.6.0.jar") \
    .config("spark.driver.extraClassPath", rootPath+"postgresql-42.6.0.jar") \
    .config("spark.executor.extraClassPath", rootPath+"postgresql-42.6.0.jar") \
    .config("spark.jars.packages","org.postgresql:postgresql:42.6.0") \
    .getOrCreate()
logger.info('SparkSession inited')


#csvPath = "/Users/simon/Downloads/задача_кандидата/res.csv"
#csvPath2 = "/Users/simon/Downloads/задача_кандидата/res2.csv"

def loadLevel(level):
    source = spark.read \
        .option("delimiter", ";") \
        .option("header", "true") \
        .csv(rootPath + "admin_lev" + str(level) + ".txt")
    # source.show(10)
    return source \
        .select( 
            col("OSM_ID"), 
            coalesce( 
                col("ADMIN_L10D"), 
                col("ADMIN_L9D"), 
                col("ADMIN_L8D"), 
                col("ADMIN_L6D"), 
                col("ADMIN_L5D"), 
                col("ADMIN_L4D"), 
                col("ADMIN_L3D"), 
                col("ADMIN_L2D") 
            ).alias("PAR_OSM_ID"),  
            col("NAME") 
        ) \
        .withColumn("LEVEL", lit(level)) \
        .withColumn("VALID_FROM", current_date()) \
        .withColumn("VALID_TO", lit("31-12-9999"))
    
result = loadLevel(2)
p = [3,4,5,6,8,9]
for i in p:
    result = result.union(loadLevel(i))

# if (os.path.exists(csvPath)):
snp = spark.read.format("jdbc")\
.option("url", "jdbc:postgresql://10.0.55.223:5432/example_db") \
.option("driver", "org.postgresql.Driver")\
.option("dbtable", "example_table") \
.option("user", "example") \
.option("password", "bigdata").load()
# snp = spark.read \
#     .format("csv") \
#     .option("header","true") \
#     .option("sep",";") \
#     .csv(csvPath)
snp.show(100,False)

snpOpen = snp \
    .filter(col("VALID_TO") == ("31-12-9999"))

snpClosed = snp \
    .filter(col("VALID_TO") != ("31-12-9999"))

newPart = result.join(snpOpen, result['OSM_ID'] == snpOpen['OSM_ID'], "full")

closed = newPart.filter(result['OSM_ID'].isNull()) \
    .select(
        snpOpen['OSM_ID'],
        snpOpen['PAR_OSM_ID'],
        snpOpen['NAME'],
        snpOpen['LEVEL'],
        snpOpen['VALID_FROM']
    ) \
    .withColumn("VALID_TO", current_date())
print("closed")
closed.show(100,False)

notChangedOrUpdated = newPart.filter(result['OSM_ID'].isNotNull() & snpOpen['OSM_ID'].isNotNull())

notChanged = notChangedOrUpdated \
    .filter(((result['NAME'] == snpOpen['NAME']) | (result['NAME'].isNull() & snpOpen['NAME'].isNull())) \
            & ((result['PAR_OSM_ID'] == snpOpen['PAR_OSM_ID']) | (result['PAR_OSM_ID'].isNull() & snpOpen['PAR_OSM_ID'].isNull()))) \
    .select(
        snpOpen['OSM_ID'],
        snpOpen['PAR_OSM_ID'],
        snpOpen['NAME'],
        snpOpen['LEVEL'],
        snpOpen['VALID_FROM'],
        snpOpen['VALID_TO']
    )
print("notChanged")
notChanged.show(100, False)
updated = notChangedOrUpdated \
    .filter(
        ((result['NAME'] != snpOpen['NAME']) & (result['NAME'].isNotNull() | snpOpen['NAME'].isNotNull())) \
        | ((result['PAR_OSM_ID'] != snpOpen['PAR_OSM_ID']) & (result['PAR_OSM_ID'].isNotNull() | snpOpen['PAR_OSM_ID'].isNotNull()))
    )
print("updated")
updated.show(100,False)
forClose = updated \
    .select(
        snpOpen['OSM_ID'],
        snpOpen['PAR_OSM_ID'],
        snpOpen['NAME'],
        snpOpen['LEVEL'],
        snpOpen['VALID_FROM']
    ) \
    .withColumn("VALID_TO", date_sub(current_date(), 1))
print("forClose")
forClose.show(100,False)

forOpen = updated \
    .select(
        result['OSM_ID'],
        result['PAR_OSM_ID'],
        result['NAME'],
        result['LEVEL'],
        result['VALID_TO']
    ) \
    .withColumn("VALID_FROM", current_date())
print("forOpen")
forOpen.show(100,False)

news = newPart.filter(snpOpen['OSM_ID'].isNull()) \
    .select(
        result['OSM_ID'],
        result['PAR_OSM_ID'],
        result['NAME'],
        result['LEVEL'],
        result['VALID_FROM'],
        result['VALID_TO']
    )
print("news")
news.show(100,False)

result = news.union(forOpen).union(forClose).union(notChanged).union(closed).union(snpClosed)
print("RESULT")
result.show(1000)

# result.repartition(1).write \
#     .format("csv") \
#     .option("header","true") \
#     .mode("overwrite") \
#     .option("sep",";") \
#     .save(csvPath2)
# try:
#     shutil.rmtree(csvPath)
# except OSError as e:
#     # If it fails, inform the user.
#     logger.error("Error: %s - %s." % (e.filename, e.strerror))

# shutil.move(csvPath2, csvPath)
# logger.info("-----Successfully write----")

result.repartition(1).write.mode("overwrite").format("jdbc")\
    .option("url", "jdbc:postgresql://10.0.55.223:5432/example_db") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "example_table") \
    .option("user", "example") \
    .option("password", "bigdata").save()

df = spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://10.0.55.223:5432/example_db") \
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", "example_table") \
    .option("user", "example") \
    .option("password", "bigdata").load()
df.show(600)
