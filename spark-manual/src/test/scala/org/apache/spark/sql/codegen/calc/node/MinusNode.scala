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

package org.apache.spark.sql.codegen.calc.node

class MinusNode(left: AlgebraNode, right: AlgebraNode)
extends BinaryNode(left, right) with AlgebraNode {

    override def generateCode(): String = {
        val gen = (left.getType(), right.getType()) match {
            case (DataType.INT, DataType.DOUBLE) =>
                s"${left.generateCode().toInt} - ${right.generateCode().toDouble}"
            case (DataType.INT, DataType.INT) =>
                s"${left.generateCode().toInt} - ${right.generateCode().toInt}"
            case (DataType.DOUBLE, DataType.INT) =>
                s"${left.generateCode().toDouble} - ${right.generateCode().toInt}"
            case (DataType.DOUBLE, DataType.DOUBLE) =>
                s"${left.generateCode().toDouble} - ${right.generateCode().toDouble}"
            case _ => throw new IllegalArgumentException()
        }
        gen
    }
}
