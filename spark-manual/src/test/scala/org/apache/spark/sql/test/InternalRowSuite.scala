// scalastyle:off
package org.apache.spark.sql.test

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, GenericRowWithSchema, UnsafeRow}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag


case class Person(id: Long, name: String)

case class Student(login: String, city: String, age: Int)

class MyObj(val i: Int)

class InternalRowSuite extends SparkFunSuite {
  
  test("kyro schema") {
	val studentEncoder = Encoders.kryo[Student]
	println(studentEncoder.schema)
  }
  
  
  test("implicit kryo encoder") {
	val spark = SparkSession.builder().master("local[2]").getOrCreate()
	import spark.implicits._
	implicit val myObjEncoder = Encoders.kryo[MyObj]
	val inputs = spark.createDataset(Seq(
	  new MyObj(1),
	  new MyObj(2)
	)).map(obj => obj.i + 1) // + 操作会转换
	
	// 对于 map filter foreach 操作是有encoder的，但是 join 就没有了
	// kryo 的schema 是 value 二进制
	inputs.show()
	spark.stop()
  }
  
  test("implicit kryo partial tuple") {
	val spark = SparkSession.builder().master("local[2]").getOrCreate()
	import spark.implicits._
	implicit def single[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)
	
	implicit def tuple2[A1, A2](implicit e1: Encoder[A1], e2: Encoder[A2]): Encoder[(A1, A2)]
	= Encoders.tuple[A1, A2](e1, e2)
	
	val d1 = spark.createDataset(Seq(new MyObj(1), new MyObj(2), new MyObj(3)))
	val d2 = d1.map(d => (d.i, d)).toDF("_1", "_2").as[(Int, MyObj)]
	d2.printSchema()
	d2.show()
  }
  
  test("kyro encoder") {
	val spark = SparkSession.builder().master("local[2]").getOrCreate()
	
	lazy implicit val studentEncoder = {
	  println("use student kyro encoder")
	  Encoders.kryo[Student]
	}
	
	implicit val personEncoder = Encoders.product[Person]
	
	//        val inputs = spark.createDataset(Seq(
	//            Student("s1", "c1", 1),
	//            Student("s2", "c2", 2)
	//        ))
	
	val inputs = spark.createDataFrame(Seq(
	  Student("s1", "c1", 1),
	  Student("s2", "c2", 2)
	))
	
	inputs.show()
	
	val people = inputs.map({
	  case Row(id: String, name: String, age: Int) => {
		Person(age, name)
	  }
	})
	
	people.show()
	spark.stop()
  }
  
  test("person encoder") {
	val personEncoder = Encoders.product[Person]
	val personExprEncoder = personEncoder.asInstanceOf[ExpressionEncoder[Person]]
	val schema = personExprEncoder.schema
	println(schema)
	val p = Person(1L, "a")
	
	// Person --> UnsafeRow
	val row = personExprEncoder.createSerializer().apply(p)
	println(row.getClass)
	
	// UnsafeRow --> Row
	val toExternalRow = CatalystTypeConverters.createToScalaConverter(schema)
	val row1 = toExternalRow(row).asInstanceOf[GenericRowWithSchema]
	println(row1.schema)
	println(row1)
	
	// UnsafeRow --> Person
	val boundEncoder = personExprEncoder.resolveAndBind()
	val p1 = boundEncoder.createDeserializer().apply(row)
	println(p1)
	
  }
  
  test("unsaferow writer") {
	val unsafeRowWriter = new UnsafeRowWriter(1)
	unsafeRowWriter.reset()
	unsafeRowWriter.zeroOutNullBytes()
	val content = UTF8String.fromString("hello world")
	unsafeRowWriter.write(0, content)
	val unsafeRow = unsafeRowWriter.getRow
	println(s"sizeInBytes:${unsafeRow.getSizeInBytes}")
	val _content = unsafeRow.getUTF8String(0)
	println(_content)
  }
  
  test("unsaferow") {
	val rowSize = UnsafeRow.calculateBitSetWidthInBytes(1) + LongType.defaultSize
	val unsafeRow = UnsafeRow.createFromByteArray(rowSize, 1)
	unsafeRow.setLong(0, 10)
	println(unsafeRow.getBaseOffset)
	val v = unsafeRow.getLong(0)
	println(v)
  }
  
  
  def printUnsafeRowBytes(unsafeRow: UnsafeRow) = {
	val sizeInBytes = unsafeRow.getSizeInBytes
	val bytes = unsafeRow.getBytes
	println(s"baseObject:${unsafeRow.getBaseObject}, " +
	  s"baseOffset:${unsafeRow.getBaseOffset}, " +
	  s"sizeInBytes:${sizeInBytes}, " +
	  s"bytes:${bytes.mkString(",")}")
  }
  
  test("simple unsaferow") {
	
	val unsafeRow = new UnsafeRow(2)
	val rowSize = UnsafeRow.calculateBitSetWidthInBytes(2) + LongType.defaultSize * 2
	unsafeRow.pointTo(new Array[Byte](rowSize), rowSize)
	printUnsafeRowBytes(unsafeRow)
	unsafeRow.setLong(0, 277)
	printUnsafeRowBytes(unsafeRow)
	unsafeRow.setLong(1, 288)
	printUnsafeRowBytes(unsafeRow)
	val v1 = unsafeRow.getLong(0)
	println(v1)
	val v2 = unsafeRow.getLong(1)
	println(v2)
  }
  
  test("schema reflection") {
	// 只是提供 Row 、InternalRow 的相互转换
	val schema = ScalaReflection.schemaFor[Student].dataType.asInstanceOf[StructType]
	println(schema)
	val toInternalRow = CatalystTypeConverters.createToCatalystConverter(schema)
	val toExternalRow = CatalystTypeConverters.createToScalaConverter(schema)
	
	val s1 = Student("L1", "C1", 10)
	
	// 转换成 InternalRow
	val row1 = toInternalRow(s1)
	println(row1.getClass)
	
	// 转换成 Row
	val row2 = toInternalRow.andThen(toExternalRow)(s1)
	println(row2.getClass)
  }
  
  
  test("row convert") {
	val schema = new StructType(Array(StructField("id", IntegerType)))
	val encoder = RowEncoder(schema).resolveAndBind()
	val internalRow = new GenericInternalRow(schema.length)
	internalRow.update(0, 20)
	val row = encoder.createDeserializer().apply(internalRow)
	println(row.getClass)
	println(row.getAs[Int](0))
	
	val internalRow1 = encoder.createSerializer().apply(row)
	println(internalRow1.getClass)
	println(internalRow1.getInt(0))
  }
  
  
  test("row get value") {
	val schema = new StructType(Array(StructField("id", IntegerType), StructField("name", StringType)))
	val row = new GenericRowWithSchema(Array(null, null), schema)
	println(row.getAs[Long]("id")) // return null
	println(row.getAs[Long](0)) // return null
	println(row.getLong(0)) // throw exception
  }
  
  case class User(id: Int, name: String)
  
  test("encoder") {
	val encoder = ExpressionEncoder[User]()
	println(encoder.schema)
  }
  
}
