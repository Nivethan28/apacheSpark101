package packages
import org.apache.spark.sql.SparkSession


class Packages_Spark {
//  Creating SparkSession
    def startSession: SparkSession ={SparkSession.builder().appName("Creating DF from Input File").master("local").getOrCreate()}
//  Reading Files
    def readingFile(spark: SparkSession, filepath: String,file_format: String) = {
    spark.read.option("header",true).option("inferSchema",true).format(file_format).load(filepath)
  }
}
