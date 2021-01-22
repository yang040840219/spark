// scalastyle:off
package org.apache.spark.parquet

import java.sql.Timestamp
import java.util.Calendar

import com.JsonUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetReader}
import org.apache.parquet.schema.MessageTypeParser
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetPartitionReaderFactory
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.parquet.{CustomParquetWriter, CustomReadSupport}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.funsuite.AnyFunSuite
import com.github.mjakubowski84.parquet4s

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

private[parquet] case class User(
	                                id: Int,
	                                name: String,
	                                score: Double,
	                                phones: Array[String],
	                                tags: Map[String, String],
	                                time: Timestamp
                                ) {
	override def toString: String = JsonUtil.object2json(this)
}

class ParquetWriteSuite extends AnyFunSuite {

	val mills = Calendar.getInstance().getTimeInMillis

	private val data = Seq[User](User(1, "a", 1.0, Array("a", "b"), Map("t1" -> "t1"), new Timestamp(mills)),
		User(2, "b", 2.0, Array("b", "c"), Map("t2" -> "t2"), new Timestamp(mills)))

	test("json") {
		data.foreach(println)

	}

	private lazy val spark = SparkSession.builder()
		.config("spark.sql.sources.useV1SourceList", "")
		.config("spark.sql.parquet.enableVectorizedReader", "false")
		.master("local[1]").getOrCreate()

	test("write parquet file") {
		val path = "file:///opt/data/parquet/user"
		spark.createDataFrame(data).write.mode(SaveMode.Overwrite).format("parquet").save(path)
	}

	test("read parquet file") {
		val path = "file:///opt/data/parquet/user"
		val df = spark.read.format("parquet").load(path)
		df.show()
		println(df.queryExecution.executedPlan)
	}

	test("read parquet with parquet4s") {
		val path = "file:///opt/data/parquet/user"
		val parquetReader = parquet4s.ParquetReader.read[User](path)
		parquetReader.toList.foreach(println)
	}

	test("write simple") {
		val path = new Path("file:///opt/data/parquet/custom/user.parquet")
		val fs = FileSystem.newInstance(new Configuration())
		fs.deleteOnExit(path)
		fs.close() // close 调用正式的delete，从cache中移除
		val schemaString =
			s"""
				 |message user {
				 |  required int32 id;
				 |  required binary name (UTF8);
				 |}
			 """.stripMargin

		val schema = MessageTypeParser.parseMessageType(schemaString)
		val parquetWriter = new CustomParquetWriter(path, schema, true, CompressionCodecName.SNAPPY)

		val data = Seq(List("1", "a"), List("2", "b"), List("3", "c")).toList
		data.foreach(item => parquetWriter.write(item.asJava))
		parquetWriter.close()
	}


	test("read simple") {
		val path = new Path("file:///opt/data/parquet/custom/user.parquet")
		val schemaString =
			s"""
				 |message user {
				 |  required int32 id;
				 |  required binary name (UTF8);
				 |}
			 """.stripMargin

		val schema = MessageTypeParser.parseMessageType(schemaString)
		val parquetReadSupport = new CustomReadSupport(schema)
		val parquetReader = ParquetReader.builder(parquetReadSupport, path).build()
		val data = new ListBuffer[String]()
		var isEmpty = false
		while (!isEmpty) {
			val item = parquetReader.read()
			if (item != null) {
				data.append(item.asScala.mkString("->"))
			} else {
				isEmpty = true
			}
		}
		parquetReader.close()
		println(data.mkString("\n"))
	}

	test("read metadata") {
		val configuration = new Configuration()
		val path = "file:///opt/data/parquet/custom/user.parquet"
		val parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(path), configuration))
		val footer = parquetFileReader.getFooter
		println(JsonUtil.object2json(footer))
	}

	test("read with spark api") {
		val path = "file:///opt/data/parquet/custom/user.parquet"

		val configuration = spark.sparkContext.hadoopConfiguration
		val dataSchema = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
		configuration.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
		configuration.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, dataSchema.json)
		configuration.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, dataSchema.json)
		configuration.set(SQLConf.PARQUET_BINARY_AS_STRING.key, "false")
		configuration.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, "true")

		val scs = new SerializableConfiguration(configuration)
		val broadcastedConf = spark.sparkContext.broadcast(scs)

		val sqlConf = spark.sessionState.conf

		val factory = ParquetPartitionReaderFactory(sqlConf, broadcastedConf, dataSchema,
			dataSchema,
			StructType(Seq.empty[StructField]),
			Array.empty)

		val fs = FileSystem.get(new Path(path).toUri, configuration)
		val fileStatus = fs.getFileStatus(new Path(path))
		val filePartition = FilePartition(0,
			Array(PartitionedFile(InternalRow.empty, path, 0, fileStatus.getLen),
				PartitionedFile(InternalRow.empty, path, 0, fileStatus.getLen)
			))

		val partitionReader = factory.createReader(filePartition)
		println(partitionReader)
		val encoder = RowEncoder(dataSchema).resolveAndBind()
		while (partitionReader.next()) {
			val internalRow = partitionReader.get()
			val row = encoder.createDeserializer().apply(internalRow)
			println(row.getInt(0), row.getString(1))
		}
	}
}
