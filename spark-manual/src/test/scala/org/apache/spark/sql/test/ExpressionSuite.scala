// scalastyle:off

package org.apache.spark.sql.test

import java.util.Properties

import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, AttributeReference, AttributeSet, ExprId, Expression, Length, Literal, MutableProjection, Substring, UnsafeRow}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.aggregate.TungstenAggregationIterator
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{SparkFunSuite, TaskContext, TaskContextImpl}

// scalastyle:off
class ExpressionSuite extends SparkFunSuite {
  
  private lazy val spark = SparkSession.builder().master("local[1]").getOrCreate()
  
  
  test("dsl expression") {
	val s = 'a.string.at(0)
	println(s.getClass)
  }
  
  test("substring expression") {
	val s = 'a.string.at(0)
	val row =  InternalRow.fromSeq(Seq("example", "example".getBytes).map(CatalystTypeConverters.convertToCatalyst))
	val expression = Substring(s, Literal.create(0, IntegerType), Literal.create(2, IntegerType))
	val value = expression.eval(row)
	println(value)
	val ctx = new CodegenContext()
	val exprCode = expression.genCode(ctx)
	val formatter = CodeFormatter.format(new CodeAndComment(exprCode.code.toString, ctx.getPlaceHolderToComments()))
	println(formatter)
  }
  
  test("length array") {
	val s = 'a.binary.at(1)
	println(s)
	val row =  InternalRow.fromSeq(Seq("example", "example".toArray.map(_.toByte))
	  .map(CatalystTypeConverters.convertToCatalyst))
	val expression = Length(s)
	val value = expression.eval(row)
	println(value)
  }
  
  test("expression") {
	val sql =
	  """
		|select (id + 1) as x, name from t1
		|where age = 10
	  """.stripMargin
	val parser = spark.sessionState.sqlParser
	val plan = parser.parsePlan(sql)
	val attributes = plan.output
	println(plan)
	println(attributes)
  }
  
  test("expression eval") {
	val x = Literal(1)
	val y = Literal(1)
	val sum = Sum(Add(x, y))
	println(sum.evaluateExpression)
	sum.eval()
  }
  
  test("aggregate expression") {
	val x = UnresolvedAttribute("x")
	val y = UnresolvedAttribute("y")
	val actual = AggregateExpression(Sum(Add(x, y)), mode = Complete, isDistinct = false).references
	val expected = AttributeSet(x :: y :: Nil)
	assert(expected == actual, s"Expected: $expected. Actual: $actual")
  }
  
  def newMutableProjection(expressions: Seq[Expression], inputSchema: Seq[Attribute],
						   useSubexprElimination: Boolean = false): MutableProjection = {
	GenerateMutableProjection.generate(expressions, inputSchema, useSubexprElimination)
  }
  
  
  test("internal row") {
	val inputIter = Seq(1, 1, 1, 1, 1, 2, 2, 2, 2).map(i => {
	  val unsafeRow = UnsafeRow.createFromByteArray(9, 1)
	  unsafeRow.setLong(0, i)
	  unsafeRow
	}).toIterator
	inputIter.foreach(row => println(row.getLong(0)))
  }
  
  
  test("attributeReference") {
	val flag = List[String]().forall(_.endsWith("xxxxx"))
	println(flag)
  }
  
  test("tungsten aggregation iterator") {
	
	val spark = SparkSession.builder().master("local[1]").appName("test")
	  .config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
	  .config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
	  .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false").getOrCreate()
	
	val sc = spark.sparkContext
	
	val taskMemoryManager = new TaskMemoryManager(sc.env.memoryManager, 0L)
	val metricsSystem = sc.env.metricsSystem
	val taskContext = new TaskContextImpl(0, 0, 0, 0L, 0, taskMemoryManager, new Properties, metricsSystem)
	TaskContext.setTaskContext(taskContext)
	
	val partIndex = 0
	// group by key
	val keyAttributeReference = AttributeReference("key", LongType)(exprId = ExprId(0))
	val groupingExpressions = Seq(keyAttributeReference)
	// count(value)
	val valueAttributeReference = AttributeReference("value", StringType)(exprId = ExprId(1))
	val countExpression = Count(valueAttributeReference)
	val aggregateExpression = AggregateExpression(countExpression, Partial, isDistinct = false)
	val aggregateExpressions = Seq(aggregateExpression)
	// count 函数结果Attribute
	val countAttributeReference = aggregateExpression.resultAttribute
	val aggregateAttributes = Seq(countAttributeReference)
	val initialInputBufferOffset = 0
	val resultExpressions = Seq(keyAttributeReference, countAttributeReference)
	
	val _newMutableProjection = (expressions: Seq[Expression],
								 inputSchema: Seq[Attribute])
	=> newMutableProjection(expressions, inputSchema, true)
	
	val originalInputAttributes = Seq(keyAttributeReference, valueAttributeReference)
	val rows = Seq((1, "a"), (1, null), (2, "a")).map(i => {
	  val unsafeRowWriter = new UnsafeRowWriter(2)
	  unsafeRowWriter.reset()
	  unsafeRowWriter.zeroOutNullBytes()
	  unsafeRowWriter.write(0, i._1.toLong)
	  if (i._2 == null) {
		unsafeRowWriter.setNullAt(1)
	  } else {
		unsafeRowWriter.write(1, UTF8String.fromString(i._2))
	  }
	  val unsafeRow = unsafeRowWriter.getRow
	  unsafeRow
	})
	
	rows.foreach(row => {
	  val key = row.getLong(0)
	  val value = if (row.isNullAt(1)) null else row.getString(1)
	  println((key, value))
	})
	
	
	val testFallbackStartsAt = None
	
	
	val numOutputRows = SQLMetrics.createMetric(sc, "numOutputRows")
	val peakMemory = SQLMetrics.createMetric(sc, "peakMemory")
	val spillSize = SQLMetrics.createMetric(sc, "spillSize")
	val avgHashProbe = SQLMetrics.createMetric(sc, "avgHashProbe")
	
	val tungstenIterator = new TungstenAggregationIterator(
	  partIndex,
	  groupingExpressions,
	  aggregateExpressions,
	  aggregateAttributes,
	  initialInputBufferOffset,
	  resultExpressions,
	  _newMutableProjection,
	  originalInputAttributes,
	  rows.toIterator,
	  testFallbackStartsAt,
	  numOutputRows,
	  peakMemory,
	  spillSize,
	  avgHashProbe
	)
	
	tungstenIterator.foreach(row => println((row.getLong(0), row.getLong(1))))
	
	spark.stop()
  }
  
}
