import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{col, collect_list, lag}

import scala.language.postfixOps

val spark = SparkSession.builder.appName("YearCountApp").master("local[*]").getOrCreate()
val data = Seq(
  ("ABC17969(AB)", "1", "ABC17969", 2022),
  ("ABC17969(AB)", "2", "CDC52533", 2022),
  ("ABC17969(AB)", "3", "DEC59161", 2023),
  ("ABC17969(AB)", "4", "F43874", 2022),
  ("ABC17969(AB)", "5", "MY06154", 2021),
  ("ABC17969(AB)", "6", "MY4387", 2022),
  ("AE686(AE)", "7", "AE686", 2022),
  ("AE686(AE)", "8", "BH2740", 2021),
  ("AE686(AE)", "9", "EG999", 2021),
  ("AE686(AE)", "10", "AE0908", 2023),
  ("AE686(AE)", "11", "QA402", 2022),
  ("AE686(AE)", "12", "OA691", 2022),
  ("AE686(AE)", "12", "OB691", 2022),
  ("AE686(AE)", "12", "OC691", 2019),
  ("AE686(AE)", "12", "OD691", 2017)
)
val columns = Seq("peer_id", "id_1", "id_2", "year")
val df = spark.createDataFrame(data).toDF(columns: _*)

def processYearCount(df: DataFrame, givenSize: Int): Array[Row] = {


  val step1 = df.filter(col("peer_id").contains(col("id_2")))
    .select("peer_id", "year").withColumnRenamed("year", "step1_year")


  val step2 = df.join(step1, Seq("peer_id"))
    .filter(col("year") <= col("step1_year"))
    .groupBy("peer_id", "year").count()


  val windowSpec = org.apache.spark.sql.expressions.Window
    .partitionBy("peer_id")
    .orderBy(col("year").desc)

  val step3 = step2
    .orderBy(col("year"))
    .withColumn("cumulative_count", functions.sum(col("count")).over(windowSpec))
    .filter((col("cumulative_count")-col("count"))<givenSize)
    .select("peer_id","year").toDF()
  step3.collect()



}


val resultDF = processYearCount(df, 5)
resultDF.foreach({ele=>{
  println("["++ele.mkString(" ")++"]");
}})


val resultDF = processYearCount(df, 7)
resultDF.foreach({ele=>{
  println("["++ele.mkString(" ")++"]");
}})