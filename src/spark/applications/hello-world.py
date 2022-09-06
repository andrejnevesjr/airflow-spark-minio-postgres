import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Hello World").getOrCreate()
sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j

LOGGER = log4jLogger.LogManager.getLogger(__name__)


LOGGER.info("Running Application")
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7])
LOGGER.info(f"RDD Count => {rdd.count()}")
