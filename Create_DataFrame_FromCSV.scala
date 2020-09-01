import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.{IntegerType,StringType,StructField,StructType}
import packages.Packages_Spark
import org.apache.spark.sql.functions._

object Create_DataFrame_FromCSV {
  def main(args: Array[String]): Unit = {
    println("Starting Application...")
//    object Creation
    val creatingDf = new Packages_Spark
    val spark = creatingDf.startSession

    spark.sparkContext.setLogLevel("ERROR")

    val csv_file_path = "C:\\Users\\NIVETHAN\\Desktop\\Databricks\\people.csv"
    val csv_file_path1 = "C:\\Users\\NIVETHAN\\Desktop\\Databricks\\creating_Dataframe\\crime-data-bos.csv"
    val csv_file_path2 = "C:\\Users\\NIVETHAN\\Desktop\\Databricks\\creating_Dataframe\\crime-data-chi.csv"

    val read_csv = creatingDf.readingFile(spark,csv_file_path,"csv")
    val read_csv1 = creatingDf.readingFile(spark,csv_file_path1,"csv")
    val read_csv2 = creatingDf.readingFile(spark,csv_file_path2,"csv")

//Using Select Function
    val select_Function = read_csv.select(read_csv.col("firstName"),read_csv.col("lastName"))
    val select_Function1 = select_Function.filter("id == 1267760")
    select_Function1.show(10,false)
//    Writing file from DataFrame
//    select_Function1.write.option("header","true").mode("overwrite").saveAsTable("output")
//    select_Function.write.format("csv").mode("overwrite").save("/tmp/delta-table")



import spark.implicits._
    read_csv.select(read_csv.col("id").alias("user_id"),$"lastName").show(10,false)

//    Using Filter Function
    val Filter1 = read_csv.filter("id == 1267760")
    Filter1.show()
    val Filter2 = read_csv.filter("salary > 80000")
    Filter2.show(5)

//    Using Select, Filter and Limit function
    val SelectAndFilter = read_csv.select("id","ssn","salary").filter("salary > 100000").limit(5)
    SelectAndFilter.show()

//    Using groupBy function and Aggregate(agg) function
    val Grouping_Func = read_csv.groupBy("salary","id","ssn")
    println(Grouping_Func.getClass)
    println(Grouping_Func.toString())
    Grouping_Func.agg(count("ssn")).show()

    val Grouping_Func1 = read_csv.groupBy("id","salary","ssn").agg(sum("firstName")).limit(5)
    Grouping_Func1.show()

//      Using orderBy function, asc and desc function, distinct
//  val Ordering_Func = read_csv.orderBy($"id".desc,col("birthDate")).limit(8)
//  Ordering_Func.show()
//  val Ordering_Func1 = read_csv.select("id","lastName","salary").distinct().limit(10)
//  Ordering_Func1.show()

//      Using joinType => inner,cross types
//  val Join_Func = read_csv1.as("bos").join(read_csv2.as("chi"),
//      ($"bos.OFFENSE_DESCRIPTION"===$"chi.description"),joinType = "inner")
//  Join_Func.show()
//  val Join_Func1 = read_csv1.as("bos").join(read_csv2.as("chi"),
//      ($"bos.LATITUDE"===$"chi.latitude"),joinType = "cross")
//  Join_Func1.show()

//      Using JoinType => outer,left,right
//  val join_Func2 = read_csv2.as("chi").jo
//  println(read_csv)

//      Using WithColumn function
//  val with_column = read_csv.withColumn("another gender",lit("M")).select("*")
//  with_column.show(15,false)
//  val with_column1 = read_csv.withColumn("bonus",lit(5000)).select("*")
//  with_column1.show(5, false)
//  val with_column2 = read_csv.withColumn("Salary_with_bonus",read_csv("salary")+ 5000).select("*")
//  with_column2.show(10,false)

//      Using Union,Intersect, except function
//  Union function can be performed only on tables having same number of columns
    val Union_func = read_csv.union(read_csv).select("*")
    Union_func.show(20, false)
//  Intersect function can be performed only on tables having same number of columns
    val Intersect_func = read_csv.intersect(read_csv).select("*")
    Intersect_func.show(20,false)
//  Except function can be performed only on tables having same number of columns
    val Except_func = read_csv.except(read_csv).select("*")
    Except_func.show()

//      Using withColumnRenamed, explain function
    val withColumn = read_csv.withColumn("tax_amount",lit(150)).select("*")
    withColumn.show()
    val withColumnRenamed = withColumn.withColumnRenamed("tax_amount","tax").select("*")
    withColumnRenamed.show()
    val withColumnRenamedFilter = withColumnRenamed.filter(withColumnRenamed("id") === "1267769").select("*")
    withColumnRenamedFilter.show()
    withColumnRenamedFilter.explain()

//      Using drop,describe,head function
    val dropFunc = withColumnRenamed.drop("birthDate")
    dropFunc.show()
    dropFunc.explain()
    val describeFunc = dropFunc.describe()
    describeFunc.show()
    println(describeFunc.head())
//    read_csv.show(10,false)
//    read_csv.printSchema()
//   val firstName = read_csv.where("firstName","Vasiliki")
//    read_csv.createOrReplaceTempView("PeopleData")
//    spark.sql("SELECT * from PeopleData WHERE PeopleData.firstName == 'Hester'").show()
//    spark.sql("SELECT * from PeopleData").show()
    spark.stop()
  }
}
