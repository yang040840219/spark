// scalastyle:off
package org.apache.spark.sql.test

import org.apache.spark.sql.{Log, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

/**
  * 2020/8/14
  */
class DataSourceSuite extends AnyFunSuite with Log {

    // scalastyle:on

     val spark = SparkSession.builder()
     .config("spark.sql.sources.useV1SourceList", "") // use datasource v2
     .config("spark.network.timeout", 100000001)
     .config("spark.executor.heartbeatInterval", 100000000)
     .config("spark.storage.blockManagerSlaveTimeoutMs", 100000001)
     .master("local[2]").appName("datasource").getOrCreate()

    test("simple datasource") {
        val df = spark.read
        .format("org.apache.spark.sql.datasource.InMemoryDataSource").load()
        df.filter("value > 2").show()
    }

    test("read parquet") {
        val path = "/opt/data/parquet/test"
        val df = spark.read.format("parquet").load(path)
        val filterDF = df.where("id > 3")
        filterDF.explain(true)
        filterDF.show()
    }

    test("write parquet") {
        import spark.implicits._
        val df = spark.createDataset(Seq(1, 2, 3, 4, 5, 6, 7)).toDF("id")
        df.write.mode(SaveMode.Overwrite).save("/data/parquet/test")
    }

    test("simple datasource partition") {
        val df = spark.read
        .format("org.apache.spark.sql.datasource.InMemoryDataSource").load()
        val partitionNum = df.rdd.getNumPartitions
        logConsole(partitionNum)
    }

    test("write datasource") {
        import spark.implicits._
        val df = spark.createDataset(Seq(("a", "a", 1))).toDF("name", "password", "role")
        df.show(truncate = false)
        df.write.format("org.apache.spark.sql.datasource.MySQLDataSource")
        .mode(SaveMode.Append).save()
    }

    test("read hive table") {
        val df = spark.read.table("default.t_test")
        df.show()
    }
}
