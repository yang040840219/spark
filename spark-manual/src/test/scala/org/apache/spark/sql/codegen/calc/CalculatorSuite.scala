// scalastyle:off

package org.apache.spark.sql.codegen.calc

import org.apache.spark.SparkFunSuite
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.codegen.calc.node.DataType
import org.apache.spark.sql.codegen.calc.node.DataType.DataType
import org.apache.spark.sql.codegen.gen.{CalculatorLexer, CalculatorParser}
import org.codehaus.janino.ExpressionEvaluator



class CalculatorSuite extends SparkFunSuite {

    test("interpreter visitor") {
        val expr = "2 + 3 * 2"
        val input = CharStreams.fromString(expr)
        val lexer = new CalculatorLexer(input)
        val tokens = new CommonTokenStream(lexer)
        val parser = new CalculatorParser(tokens)
        val root = parser.input()
        val variables = Map.empty[String, Any]
        val calcVisitor = new InterpreterVisitor(variables)
        val result = calcVisitor.visit(root)
        // scalastyle:off
        println(result)
    }

    test("validate visitor") {
        val expr = "2.0 + 3 * 2"
        val input = CharStreams.fromString(expr)
        val lexer = new CalculatorLexer(input)
        val tokens = new CommonTokenStream(lexer)
        val parser = new CalculatorParser(tokens)
        val root = parser.input()
        val variableTypes = Map.empty[String,DataType]
        val calcVisitor = new ValidateVisitor(variableTypes)
        val node = calcVisitor.visit(root)
        val code = node.generateCode()
        // scalastyle:off
        println(s"code:$code")
        // scalastyle:on
        val evaluator = new ExpressionEvaluator()
        val expressionType = if (node.getType() == DataType.INT) {
            classOf[Int]
        } else {
            classOf[Double]
        }
        evaluator.setExpressionType(expressionType)
        evaluator.cook(code)
        val result = evaluator.evaluate(Array.empty)
        // scalastyle:off
        println(result)
        // scalastyle:on
    }

}
