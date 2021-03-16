/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.codegen.calc

import org.apache.spark.sql.codegen.gen.CalculatorBaseVisitor
import org.apache.spark.sql.codegen.gen.CalculatorParser._

class InterpreterVisitor(variables: Map[String, Any]) extends CalculatorBaseVisitor[Any] {


    override def visitCalculate(ctx: CalculateContext): Any = {
        visit(ctx.plusOrMinus())
    }

    override def visitToMultOrDiv(ctx: ToMultOrDivContext): Any = super.visitToMultOrDiv(ctx)

    override def visitPlus(ctx: PlusContext): Any = {
        val left = visit(ctx.plusOrMinus())
        val right = visit(ctx.multOrDiv())
        (left, right) match {
            case (l: Int, r: Int) => l + r
            case (l: Double, r: Int) => l + r
            case (l: Double, r: Double) => l + r
            case _ => throw new IllegalArgumentException()
        }
    }

    override def visitMinus(ctx: MinusContext): Any = {
        val left = visit(ctx.plusOrMinus())
        val right = visit(ctx.multOrDiv())
        (left, right) match {
            case (l: Int, r: Int) => l - r
            case (l: Int, r: Double) => l - r
            case (l: Double, r: Int) => l - r
            case (l: Double, r: Double) => l - r
            case _ => throw new IllegalArgumentException()
        }
    }

    override def visitToUnaryMinus(ctx: ToUnaryMinusContext): Any = super.visitToUnaryMinus(ctx)


    override def visitMultiplication(ctx: MultiplicationContext): Any = {
        val left = visit(ctx.multOrDiv())
        val right = visit(ctx.unaryMinus())
        (left, right) match {
            case (l: Int, r: Int) => l * r
            case (l: Int, r: Double) => l * r
            case (l: Double, r: Int) => l * r
            case (l: Double, r: Double) => l * r
            case _ => throw new IllegalArgumentException()
        }
    }

    override def visitDivision(ctx: DivisionContext): Any = {
        val left = visit(ctx.multOrDiv())
        val right = visit(ctx.unaryMinus())
        (left, right) match {
            case (l: Int, r: Int) => l / r
            case (l: Int, r: Double) => l / r
            case (l: Double, r: Int) => l / r
            case (l: Double, r: Double) => l / r
            case _ => throw new IllegalArgumentException()
        }
    }

    override def visitChangeSign(ctx: ChangeSignContext): Any = {
        val value = visit(ctx.unaryMinus())
        value match {
            case i: Int => -1 * i
            case d: Double => -1.0 * d
        }
    }

    override def visitToAtom(ctx: ToAtomContext): Any = super.visitToAtom(ctx)

    override def visitDouble(ctx: DoubleContext): Any = {
        ctx.DOUBLE().getText.toDouble
    }

    override def visitInt(ctx: IntContext): Any = {
        ctx.INT().getText.toInt
    }

    override def visitVariable(ctx: VariableContext): Any = {
        val variable = ctx.ID().getText
        variables(variable)
    }

    override def visitBraces(ctx: BracesContext): Any = {
        visit(ctx.plusOrMinus())
    }

    override def visitFunction(ctx: FunctionContext): Any = super.visitFunction(ctx)

}