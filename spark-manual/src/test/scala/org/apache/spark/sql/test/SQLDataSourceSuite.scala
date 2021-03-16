// scalastyle:off

package org.apache.spark.sql.test

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkFunSuite}


class SQLDataSourceSuite extends SparkFunSuite {

    def withSparkSession(f: SparkSession => Unit) : Unit = {
        val sparkConf = new SparkConf(false)
        .setMaster("local[2]")
        .setAppName("test")
        .set("spark.ui.enabled", "true")
        .set(SQLConf.SHUFFLE_PARTITIONS.key, "50")
        .set("spark.sql.adaptive.enabled", "false")
        .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false") // code gen
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
        try
            f(spark)
        catch {
            case e:Exception => e.printStackTrace()
        }finally spark.stop()
    }

    def withSparkSession(f: SparkSession => Unit, targetNumPostShufflePartitions: Int,
                         minNumPostShufflePartitions: Option[Int]): Unit = {
        val sparkConf = new SparkConf(false)
        .setMaster("local[4]")
        .setAppName("test")
        .set("spark.ui.enabled", "true")
        .set("spark.driver.allowMultipleContexts", "false")
        .set(SQLConf.SHUFFLE_PARTITIONS.key, "50")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key , "-1")
        .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false") // code gen
        .set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, targetNumPostShufflePartitions.toString)


        minNumPostShufflePartitions match {
            case Some(numPartitions) =>
                sparkConf.set(SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key, numPartitions.toString)
            case None =>
                sparkConf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, "-1")
        }

        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        spark.sparkContext.setLogLevel("INFO")

        try f(spark) finally spark.stop()

    }


    test("aggregate count") {
        val spark = SparkSession.builder().master("local[1]").appName("test")
        .config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
        .config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
        .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        .config("spark.network.timeout", 100000001)
        .config("spark.executor.heartbeatInterval", 100000000)
        .getOrCreate()
        import spark.implicits._
        val df = Seq((1,"a"),(1,"b"),(1,null),(2,"a"),(2,"a")).toDF("key","value")
        val result = df.groupBy("key").agg(count(col("value")))
        val qe = result.queryExecution
        qe.analyzed.collect{
            case agg:Aggregate => println((agg.groupingExpressions, agg.aggregateExpressions))
        }
        //result.show()
    }

    test("aggregate shuffle") {
        val spark = SparkSession.builder().master("local[2]").appName("test")
        .config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
        .config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
        .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        .config("spark.network.timeout", 100000001)
        .config("spark.executor.heartbeatInterval", 100000000)
        .getOrCreate()
        val df = spark.range(100l, 110l, 1, numPartitions = 5)
        .selectExpr("id % 3 as key", "id as value")
        val agg = df.groupBy("key").count()

        val qe = agg.queryExecution

        qe.analyzed.collect{
            case agg:Aggregate => println((agg.groupingExpressions, agg.aggregateExpressions))
        }
        println(qe.analyzed)
        println(qe.optimizedPlan)
        println(qe.sparkPlan)
        println(qe.executedPlan)

        qe.executedPlan.foreach {
            case agg: HashAggregateExec =>
                println(agg)
            case _ =>
        }

        agg.show()
        spark.stop()
    }


    test("test single plan") {
        val output = StructType(StructField("id", LongType, nullable = false) :: Nil).toAttributes
    }

    test("parallel rdd") {
        val spark = SparkSession.builder().master("local[2]").appName("test")
        .config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
        .config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
        .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        .config("spark.network.timeout", 100000001)
        .config("spark.executor.heartbeatInterval", 100000000)
        .getOrCreate()
        val rdd = spark.sparkContext.parallelize(0 until 3, 3)
        rdd.mapPartitionsWithIndex({
            case (index, iterator) => Seq((index, iterator.mkString("|"))).toIterator
        }).collect().foreach(println)
    }

    test("determining the number of reducers of aggregate") {
        val aggregate = (spark:SparkSession) => {
            val df = spark.range(0, 10000, 1)
            .selectExpr("id % 100 as key",  "id as value")
            val agg = df.groupBy("key").count()
            agg.show()

            val exchanges = agg.queryExecution.executedPlan.collect {
                case e: ShuffleExchangeExec => e
            }
            exchanges.foreach(exchange => println(exchange.outputPartitioning.numPartitions))

            TimeUnit.SECONDS.sleep(10000)
        }

        //withSparkSession(aggregate, 1024*1024, None) // 1M

        withSparkSession(aggregate)
    }

    test("sql write dir") {
        withSparkSession(spark => {
            val df = spark.range(0, 1000, 1, numPartitions = 5).selectExpr("id % 3 as key", "id as value")

            //val writeDF = df.coalesce(2)
            //val writeDF = df.repartition(20)
            //val writeDF = df.repartition(20, col("value"))

            val toInt = udf[Int, Double](x => Math.abs(x.toInt))

            val writeDF = df.withColumn("rand", toInt(randn(4))).repartition(10, col("key"), col("rand"))

            writeDF.show()
            writeDF.explain(true)

            writeDF.write.partitionBy("key").mode(SaveMode.Overwrite).save("file:///data/tmp/spark-sql/t_1")

        })
    }


    test("delta create table") {
        withSparkSession(spark => {
            val sql =
                s"""
                   |create table default.t_bicall_log USING DELTA LOCATION '/data/delta/bicall_log'
                 """.stripMargin

            spark.read.parquet("")

            spark.sql(sql)
        })
    }

}
