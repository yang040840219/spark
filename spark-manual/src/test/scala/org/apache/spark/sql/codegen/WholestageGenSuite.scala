// scalastyle:off
package org.apache.spark.sql.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{BoundReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.{FilterExec, LocalTableScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Print, SaveMode, SparkSession}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}


class WholestageGenSuite extends SparkFunSuite {

    test("write data") {
        val spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.SHUFFLE_PARTITIONS.key, "2")
        .getOrCreate()
        val df = spark.createDataFrame(Seq(("a", "1"), ("b", "2"), ("c", "3"))).toDF("id", "age")
        df.write.mode(SaveMode.Overwrite).save("/opt/data/spark/id")
    }

    def genDF(): DataFrame = {
        val spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.SHUFFLE_PARTITIONS.key, "2")
        .getOrCreate()
        val df = spark.read.load("/opt/data/spark/id/*")
        df
    }

    def simple(spark: SparkSession): Unit = {
        import spark.implicits._
        val df = spark.read.load("/opt/data/spark/id/*")
        val agg = df.filter(col("age") > 2)
        .map(row => (row.getAs[String]("id"), row.getAs[String]("age")))
        .toDF("id", "age").groupBy("id").agg(max("age").alias("age"))

        // val qe = agg.queryExecution

        // Print.printConsole(qe.analyzed)
        // Print.printConsole(qe.optimizedPlan)
        // Print.printConsole(qe.sparkPlan)
        // Print.printConsole(qe.executedPlan)

        // Print.printConsole(qe.executedPlan)

        agg.show()

        // agg.explain(true)

        // Print.printConsole("---------------debug-----------")
        // agg.debug()
        // Print.printConsole("---------------debug code gen-----------")
        // agg.debugCodegen()

        // agg.debugCodegen()

    }

    test("filter exec simple") {
        val df = genDF()
        df.filter(col("age") > 2).show()
    }

    test("without codegen") {
        val spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
        .config(SQLConf.SHUFFLE_PARTITIONS.key, "2")
        .getOrCreate()
        simple(spark)
    }

    test("wholestage codegen") {
        val spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.SHUFFLE_PARTITIONS.key, "2")
        .config("spark.network.timeout", 100000001)
        .config("spark.executor.heartbeatInterval", 100000000)
        .config("spark.storage.blockManagerSlaveTimeoutMs", 100000001)
        .getOrCreate()
        simple(spark)
        // TimeUnit.DAYS.sleep(1)
    }

    test("sql codegen") {
        val spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.SHUFFLE_PARTITIONS.key, "2")
        .getOrCreate()
        spark.sql("SELECT 1").show()
    }

    test("filter exec gen") {
        // 不能执行，报 unresloved 异常
        val spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.SHUFFLE_PARTITIONS.key, "2")
        .getOrCreate()
        val e1 = Literal(2)
        val e2 = Literal(2)
        val expr = EqualTo(e1, e2)
        val schema = new StructType()
        .add(StructField("id", StringType))
        .add(StructField("value", StringType))
        val attributeSeq = schema.toAttributes
        val converter = CatalystTypeConverters.createToCatalystConverter(schema)
        val data = Seq(("a", "1"), ("a", "2")).map(converter(_).asInstanceOf[InternalRow])
        val scanExec = LocalTableScanExec(attributeSeq, data)
        val filterExec = FilterExec(expr, scanExec)
        val wholeStageCodegenExec = WholeStageCodegenExec(filterExec)(1)
        val (ctx, cleanedSource) = wholeStageCodegenExec.doCodeGen()
        Print.printConsole(cleanedSource.body)
        val references = ctx.references.toArray
        Print.printConsole("----reference---")
        references.foreach(Print.printConsole)
        val rdd = wholeStageCodegenExec.doExecute()
        rdd.collect().foreach(Print.printConsole)
    }

    object AlwaysNull extends InternalRow {
        override def numFields: Int = 1
        override def setNullAt(i: Int): Unit = {}
        override def copy(): InternalRow = this
        override def anyNull: Boolean = true
        override def isNullAt(ordinal: Int): Boolean = true
        override def update(i: Int, value: Any): Unit = notSupported
        override def getBoolean(ordinal: Int): Boolean = notSupported
        override def getByte(ordinal: Int): Byte = notSupported
        override def getShort(ordinal: Int): Short = notSupported
        override def getInt(ordinal: Int): Int = notSupported
        override def getLong(ordinal: Int): Long = notSupported
        override def getFloat(ordinal: Int): Float = notSupported
        override def getDouble(ordinal: Int): Double = notSupported
        override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = notSupported
        override def getUTF8String(ordinal: Int): UTF8String = notSupported
        override def getBinary(ordinal: Int): Array[Byte] = notSupported
        override def getInterval(ordinal: Int): CalendarInterval = notSupported
        override def getStruct(ordinal: Int, numFields: Int): InternalRow = notSupported
        override def getArray(ordinal: Int): ArrayData = notSupported
        override def getMap(ordinal: Int): MapData = notSupported
        override def get(ordinal: Int, dataType: DataType): AnyRef = notSupported
        private def notSupported: Nothing = throw new UnsupportedOperationException
    }

    test("generate unsafe projection") {
        val dataType = (new StructType).add("a", StringType)
        val exprs = BoundReference(0, dataType, nullable = true) :: Nil
        val projection = GenerateUnsafeProjection.generate(exprs)
        Print.printConsole(projection)
        val result = projection.apply(InternalRow(AlwaysNull))
        Print.printConsole(result)
    }

}
