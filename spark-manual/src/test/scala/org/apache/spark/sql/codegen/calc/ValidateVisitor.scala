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

import org.apache.spark.sql.codegen.calc.node._
import org.apache.spark.sql.codegen.calc.node.DataType.DataType
import org.apache.spark.sql.codegen.gen.CalculatorBaseVisitor
import org.apache.spark.sql.codegen.gen.CalculatorParser._


class ValidateVisitor(variablesTypes: Map[String, DataType])
extends CalculatorBaseVisitor[AlgebraNode] {

    override def visitCalculate(ctx: CalculateContext): AlgebraNode = {
        visit(ctx.plusOrMinus())
    }

    override def visitToMultOrDiv(ctx: ToMultOrDivContext): AlgebraNode =
        super.visitToMultOrDiv(ctx)

    override def visitPlus(ctx: PlusContext): AlgebraNode = {
        new PlusNode(visit(ctx.plusOrMinus()), visit(ctx.multOrDiv()))
    }

    override def visitMinus(ctx: MinusContext): AlgebraNode = {
        new MinusNode(visit(ctx.plusOrMinus()), visit(ctx.multOrDiv()))
    }

    override def visitMultiplication(ctx: MultiplicationContext): AlgebraNode = {
        new MultiplicationNode(visit(ctx.multOrDiv()), visit(ctx.unaryMinus()))
    }

    override def visitToUnaryMinus(ctx: ToUnaryMinusContext): AlgebraNode =
        super.visitToUnaryMinus(ctx)

    override def visitDivision(ctx: DivisionContext): AlgebraNode = super.visitDivision(ctx)

    override def visitChangeSign(ctx: ChangeSignContext): AlgebraNode = {
        new ChangeSignNode(visit(ctx.unaryMinus()))
    }

    override def visitToAtom(ctx: ToAtomContext): AlgebraNode = super.visitToAtom(ctx)

    override def visitDouble(ctx: DoubleContext): AlgebraNode = {
        new LiteralNode(ctx.DOUBLE().getText.toDouble)
    }

    override def visitInt(ctx: IntContext): AlgebraNode = {
        new LiteralNode(ctx.INT().getText.toInt)
    }

    override def visitVariable(ctx: VariableContext): AlgebraNode = super.visitVariable(ctx)

    override def visitBraces(ctx: BracesContext): AlgebraNode = super.visitBraces(ctx)

    override def visitFunction(ctx: FunctionContext): AlgebraNode = super.visitFunction(ctx)

    override def visitFunc(ctx: FuncContext): AlgebraNode = super.visitFunc(ctx)
}
