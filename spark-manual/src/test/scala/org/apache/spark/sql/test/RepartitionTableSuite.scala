// scalastyle:off
package org.apache.spark.sql.test

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col


class RepartitionTableSuite extends  SparkFunSuite{

    lazy val spark = {
        val spark = SparkSession.builder().master("local[2]")
        .config("spark.network.timeout", "1000000s")
        //.config("spark.sql.files.maxRecordsPerFile", "1000")
        .appName("repartition table").getOrCreate()
        spark
    }

    test("spark partition file num") {
        import spark.implicits._
        val df = spark.range(0, 10000, 1, numPartitions = 100).selectExpr("id % 3 as key", "id as value")
        val writeDF = df.repartition($"key")
        writeDF.write.partitionBy("key")
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save("file:///opt/data/tmp/spark-sql/t_1")
    }

    test("repartition with multi partition columns") {
        val df = spark.range(0, 10000, 1, numPartitions = 20).selectExpr("id",
            "id % 3 as p_day",
            "if(id > 8000, id % 3, 0) as p_hour")
        // val writeDF = df.repartition(15, Seq(col("p_day"), col("p_hour")):_*)
        val writeDF = df.repartition(15)
        writeDF.write.partitionBy("p_day", "p_hour")
          .format("json")
          .mode(SaveMode.Overwrite)
          .save("file:///opt/data/tmp/spark-sql/t_3")
    }

    test("simple repartition") {
        import spark.implicits._
        val df = spark.range(0, 10000, step = 1, numPartitions = 100).selectExpr("id % 3 as key", "id as value")
        println(df.rdd.partitions.length)
        // df.repartition
        df.coalesce(2).write.mode(SaveMode.Overwrite).save("file:///opt/data/tmp/spark-sql/t_1")
    }

    test("read text and repartition") {
        import spark.implicits._
        val df = spark.read.textFile("file:///opt/data/tmp/spark-sql/t_2")
          .map(x => {
                val a = x.split(",")
              (a(0).toInt, a(1))
          }).toDF("id", "value")
        println(df.rdd.partitions.length)
        df.repartition(2).write.mode(SaveMode.Overwrite).save("file:///opt/data/tmp/spark-sql/t_1")

    }

    test("spark repartition on columns") {
        val spark = SparkSession.builder().master("local[*]")
        .config("spark.sql.files.maxRecordsPerFile", "10")
        .appName("repartition").getOrCreate()

        val df = spark.range(1, 100).toDF("value").selectExpr("(value % 3) as value", "value as name")
        .repartition(col("value"))

        df.write.mode(SaveMode.Overwrite).save("/opt/data/delta/t_p")

        TimeUnit.DAYS.sleep(1)

    }

}
