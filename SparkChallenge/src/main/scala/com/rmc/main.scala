package com.rmc

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

import java.io.File
import java.util.Properties
import scala.io.Source

object main  {

  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:/winutils")
    val logger = Logger.getLogger(this.getClass.getName)
    logger.trace("Inicio APP")
    logger.trace("Setup...")

    val url = getClass.getResource("/application.properties")
    val properties = new Properties()
    val source = Source.fromURL(url)
    properties.load(source.bufferedReader())

    val googlePlayStore=properties.getProperty("google_play_store_file_name")
    val userReviewsFile = properties.getProperty("user_reviews_file_name")
    val fileFolderPath = properties.getProperty("files_folder_path")
    val tempFolderPath = properties.getProperty("temp_folder_path")



    val spark = SparkSession.builder
      .master("local") // Change it as per your cluster
      .appName("Spark CSV Reader")
      .getOrCreate;


    //Ler googleplaystore.csv
    val dfGPS = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("delimiter", ",")
      .load(getClass.getResource("/"+googlePlayStore).toString)

    //Ler googleplaystore_user_reviews.csv
    dfGPS.createOrReplaceTempView("googleplaystore")
    val dfUR = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("delimiter", ",")
      .load(getClass.getResource("/"+userReviewsFile).toString)
    dfUR.createOrReplaceTempView("googleplaystore_user_reviews")

    logger.trace("################### PART 1 ###############")

    val df_1= spark.sql("SELECT  App,IFNULL(AVG(Sentiment_Polarity),0) AS Average_Sentiment_Polarity FROM googleplaystore_user_reviews GROUP BY App")
    df_1.show()

    logger.trace("################### PART 2 ###############")

    val df_2 = spark.sql("SELECT * FROM googleplaystore WHERE Rating>=4 ORDER BY Rating DESC")
    df_2.show()
    df_2.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "§")
      .option("header", "true")
      .option("quote", "\"")
      .csv(tempFolderPath+"/best_apps")
    rewriteTempFile("best_apps",spark.sparkContext,tempFolderPath,fileFolderPath)

    logger.trace("################### PART 3 ###############")


   val df_gpsFormatted=dfGPS
     .withColumn("Rating",dfGPS("Rating").cast(DoubleType) )
     .withColumn("Reviews",coalesce(dfGPS("Reviews"),lit(0)).cast(LongType) )
     .withColumn("Size",dfGPS("Size").substr(lit(0),length(dfGPS("Size")) -1).cast(DoubleType))
     .withColumn("Genres",functions.split(dfGPS("Genres"),";"))
     .withColumn("Price",when(dfGPS("Price")===lit(0),lit(0)).otherwise(dfGPS("Price").substr(lit(2),length(dfGPS("Price")))*0.9))
     .withColumnRenamed("Content Rating","Content_Rating")
     .withColumn("Last Updated",to_date(dfGPS("Last Updated"),"MMM dd, yyyy") )
     .withColumnRenamed("Last Updated","Last_Updated")
     .withColumnRenamed("Current Ver","Current_Version")
     .withColumnRenamed("Android Ver","Minimum_Android_Version")


   val df_3=  df_gpsFormatted.groupBy("App")
     .agg(collect_list(df_gpsFormatted("Category")).as("Categories")
       ,max(df_gpsFormatted("Reviews")).as("Reviews")).as("df_3")
     .join(df_gpsFormatted.as("df_gpsFormatted"),col("df_gpsFormatted.App") === col("df_3.App") && col("df_gpsFormatted.Reviews") === col("df_3.Reviews"))
     .select("df_3.App"
       ,"df_gpsFormatted.Rating"
       ,"df_gpsFormatted.Reviews"
       ,"df_gpsFormatted.Size"
       ,"df_gpsFormatted.Installs"
       ,"df_gpsFormatted.Type"
       ,"df_gpsFormatted.Price"
       ,"df_gpsFormatted.Content_Rating"
       ,"df_gpsFormatted.Genres"
       ,"df_gpsFormatted.Last_Updated"
       ,"df_gpsFormatted.Current_Version"
       ,"df_gpsFormatted.Minimum_Android_Version"
     )

    df_3.show(100)
    df_3.printSchema()

    logger.trace("################### PART 4 ###############")

    val df_gpsCleaned = df_3
      .join(df_1.as("df_1"),col("df_1.App") === col("df_3.App"))
      .drop(col("df_1.App"))//Eliminar coluna duplicada

    df_gpsCleaned.show()
    df_gpsCleaned.printSchema()

    df_gpsCleaned
      .write
      .mode(SaveMode.Overwrite)
      .option("compression","gzip")
      .parquet(fileFolderPath+"/googleplaystore_cleaned")

    logger.trace("################### PART 5 ###############")

    val df_3_Exploded = df_3.
      withColumn("Genre", explode(col("Genres")))

    df_3_Exploded.show(100)
    val df_4 = df_3_Exploded
      .join(df_1.as("df_1"),col("df_1.App") === col("df_3.App"))
      .groupBy("Genre")
      .agg(
        count(df_3_Exploded("Genre")).as("Count"),
        avg(df_3_Exploded("Rating")).as("Average_Rating"),
        avg(df_1("Average_Sentiment_Polarity")).as("Average_Sentiment_Polarity")
      )
    df_4.show(1000)
    df_4
      .write
      .mode(SaveMode.Overwrite)
      .option("compression","gzip")
      .parquet(fileFolderPath+"/googleplaystore_metrics")
  }

  def rewriteTempFile(dirName:String, sc:SparkContext,tempFolderPath:String,fileFolderPath:String ):Unit = {
    // Copy the actual file from Directory and Renames to custom name
    val hdfs = FileSystem.get(sc.hadoopConfiguration)

    val srcPath=new Path(tempFolderPath+"/"+dirName)
    val destPath= new Path(fileFolderPath+"/"+dirName+".csv")
    val srcFile=FileUtil.listFiles(new File(tempFolderPath+"/"+dirName))
      .filter(f=>f.getPath.endsWith(".csv"))(0)
    hdfs.delete(destPath,true)
    //Copy the CSV file outside of Directory and rename
    FileUtil.copy(srcFile,hdfs,destPath,true,sc.hadoopConfiguration)
    //Removes CRC File that create from above statement
    hdfs.delete(new Path(fileFolderPath+"/."+dirName+".csv.crc"),true)
    //Remove Directory created by df.write()
    hdfs.delete(srcPath,true)
  }

}
