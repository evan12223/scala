import org.apache.spark.sql

def processYearCount(df: DataFrame, givenSize: Int): DataFrame = {


  val step1 = df.filter(col("peer_id").contains(col("id_2")))
    .select("peer_id", "year").withColumnRenamed("year", "step1_year")
  step1.show(truncate = false)

  val step2 = df.join(step1, Seq("peer_id"))
    .filter(col("year") <= col("step1_year"))
    .groupBy("peer_id", "year").count()
  step2.show(truncate = false)

  val windowSpec = org.apache.spark.sql.expressions.Window
    .partitionBy("peer_id")
    .orderBy(col("year").desc)

  val step3=df
    .orderBy(col("year"))
    .withColumn("cumulative_count", sum("count").over(windowSpec))
    .filter(col("cumulative_count") - col("count") < givenSize)
    .select("peer_id", "year")
}

println("Dd")