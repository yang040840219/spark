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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the sum calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (5), (10), (15) AS tab(col);
       30
      > SELECT _FUNC_(col) FROM VALUES (NULL), (10), (15) AS tab(col);
       25
      > SELECT _FUNC_(col) FROM VALUES (NULL), (NULL) AS tab(col);
       NULL
  """,
  group = "agg_funcs",
  since = "1.0.0")
case class Sum(child: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes
  with UnaryLike[Expression] {

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(NumericType, YearMonthIntervalType, DayTimeIntervalType))

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForAnsiIntervalOrNumericType(child.dataType, "sum")

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _: IntegralType => LongType
    case _: YearMonthIntervalType => YearMonthIntervalType
    case _: DayTimeIntervalType => DayTimeIntervalType
    case _ => DoubleType
  }

  private lazy val sum = AttributeReference("sum", resultType)()

  private lazy val isEmpty = AttributeReference("isEmpty", BooleanType, nullable = false)()

  private lazy val zero = Literal.default(resultType)

  override lazy val aggBufferAttributes = resultType match {
    case _: DecimalType => sum :: isEmpty :: Nil
    case _ => sum :: Nil
  }

  override lazy val initialValues: Seq[Expression] = resultType match {
    case _: DecimalType => Seq(zero, Literal(true, BooleanType))
    case _ => Seq(Literal(null, resultType))
  }

  override lazy val updateExpressions: Seq[Expression] = {
    resultType match {
      case _: DecimalType =>
        // For decimal type, the initial value of `sum` is 0. We need to keep `sum` unchanged if
        // the input is null, as SUM function ignores null input. The `sum` can only be null if
        // overflow happens under non-ansi mode.
        val sumExpr = if (child.nullable) {
          If(child.isNull, sum, sum + KnownNotNull(child).cast(resultType))
        } else {
          sum + child.cast(resultType)
        }
        // The buffer becomes non-empty after seeing the first not-null input.
        val isEmptyExpr = if (child.nullable) {
          isEmpty && child.isNull
        } else {
          Literal(false, BooleanType)
        }
        Seq(sumExpr, isEmptyExpr)
      case _ =>
        // For non-decimal type, the initial value of `sum` is null, which indicates no value.
        // We need `coalesce(sum, zero)` to start summing values. And we need an outer `coalesce`
        // in case the input is nullable. The `sum` can only be null if there is no value, as
        // non-decimal type can produce overflowed value under non-ansi mode.
        if (child.nullable) {
          Seq(coalesce(coalesce(sum, zero) + child.cast(resultType), sum))
        } else {
          Seq(coalesce(sum, zero) + child.cast(resultType))
        }
    }
  }

  /**
   * For decimal type:
   * If isEmpty is false and if sum is null, then it means we have had an overflow.
   *
   * update of the sum is as follows:
   * Check if either portion of the left.sum or right.sum has overflowed
   * If it has, then the sum value will remain null.
   * If it did not have overflow, then add the sum.left and sum.right
   *
   * isEmpty:  Set to false if either one of the left or right is set to false. This
   * means we have seen atleast a value that was not null.
   */
  override lazy val mergeExpressions: Seq[Expression] = {
    resultType match {
      case _: DecimalType =>
        val bufferOverflow = !isEmpty.left && sum.left.isNull
        val inputOverflow = !isEmpty.right && sum.right.isNull
        Seq(
          If(
            bufferOverflow || inputOverflow,
            Literal.create(null, resultType),
            // If both the buffer and the input do not overflow, just add them, as they can't be
            // null. See the comments inside `updateExpressions`: `sum` can only be null if
            // overflow happens.
            KnownNotNull(sum.left) + KnownNotNull(sum.right)),
          isEmpty.left && isEmpty.right)
      case _ => Seq(coalesce(coalesce(sum.left, zero) + sum.right, sum.left))
    }
  }

  /**
   * If the isEmpty is true, then it means there were no values to begin with or all the values
   * were null, so the result will be null.
   * If the isEmpty is false, then if sum is null that means an overflow has happened.
   * So now, if ansi is enabled, then throw exception, if not then return null.
   * If sum is not null, then return the sum.
   */
  override lazy val evaluateExpression: Expression = resultType match {
    case d: DecimalType =>
      If(isEmpty, Literal.create(null, resultType),
        CheckOverflowInSum(sum, d, !SQLConf.get.ansiEnabled))
    case _ => sum
  }

  override protected def withNewChildInternal(newChild: Expression): Sum = copy(child = newChild)
}
