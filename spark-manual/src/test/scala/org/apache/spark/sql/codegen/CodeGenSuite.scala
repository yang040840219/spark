// scalastyle:off

package org.apache.spark.sql.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext}
import org.apache.spark.sql.catalyst.expressions.{Add, Cast, Literal, Multiply}
import org.apache.spark.sql.codegen.calc.node.{DataType, LiteralNode}
import org.apache.spark.sql.types.DoubleType


// scalastyle:off
class CodeGenSuite extends SparkFunSuite {

    test("codeGenContext") {
        val ctx = new CodegenContext()
        val expressions = "hello".expr.as("world") :: "hello".expr.as("world") :: Nil
        val codes = ctx.generateExpressions(expressions, doSubexpressionElimination = true)
        println(codes)
    }

    test("code gen for add operator") {
        val ctx = new CodegenContext()
        val str = Literal("10")
        val dec = Literal(10.12)
        val cast = Cast(str, DoubleType)
        val add = Add(cast, dec)
        val exprCode = add.genCode(ctx)
        println(exprCode)
        val result = add.eval()
        println(result)
        val formatter = CodeFormatter.format(new CodeAndComment(exprCode.code.toString, ctx.getPlaceHolderToComments()))
        println(formatter)
    }

    test("code gen multi operator") {
        val ctx = new CodegenContext()
        val num1 = Literal(1)
        val num2 = Literal(2)
        val num3 = Literal(3)
        val expr = Multiply(Add(num1, num2), num3)
        val exprCode = expr.genCode(ctx)
        val result = expr.eval()
        println(result)
        val formatter = CodeFormatter.format(new CodeAndComment(exprCode.code.toString, ctx.getPlaceHolderToComments()))
        println(formatter)
    }

    test("match enum") {
        val x = new LiteralNode(1)
        val y = new LiteralNode(2)
        (x.getType(), y.getType()) match {
            case (DataType.INT, DataType.INT) => println("hello")
        }
    }

}
