from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def main():
	spark=SparkSession.builder\
		.appName("GCS to hive read and write")\
		.enableHiveSupport()\
		.getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")
	conf = spark.sparkContext._jsc.hadoopConfiguration()
	conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
	conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

	print("Use Spark Application to Read csv data from cloud GCS and get a DF created with the GCS data in the on prem, "
		 "convert csv to json in the on prem DF and store the json into new cloud GCS location")
	cust_schema=StructType([ StructField("id",IntegerType(),False),
							 StructField("custfname",StringType(), False),
							 StructField("custlname",StringType(),True),
							 StructField("custage",IntegerType(),True),
							 StructField("custprofession",StringType(),True)])
	gcs_df=spark.read.csv("gs://inceptez-data-store-yuvaraj/dataset/custs",mode="dropmalformed",schema=cust_schema)
	gcs_df.show(10)
	print("GCS Read Completed Successfully")
	gcs_df.write.mode("overwrite").partitionBy("custage").saveAsTable("default.cust_info_gcs")
	print("GCS to hive table load Completed Successfully")

	print("Hive to GCS usecase starts here")
	gcs_df1=spark.read.table("default.cust_info_gcs")
	curts=spark.createDataFrame([1], IntegerType()).withColumn("curts", current_timestamp()).select(date_format(col("curts"),"yyyyMMddHHmmSS")).first()[0]
	print(curts)
	gcs_df1.repartition(2).write.json("gs://inceptez-data-store-yuvaraj/dataset/cust_output_json_"+curts)
	print("gcs Write Completed Successfully")

	print("Hive to GCS usecase starts here")
	gcs_df2=spark.read.table("default.cust_info_gcs")
	gcs_df2.repartition(2).write.mode("overwrite").csv("gs://inceptez-data-store-yuvaraj/dataset/cust_csv")
	print("gcs write completed sucessfully")

main()