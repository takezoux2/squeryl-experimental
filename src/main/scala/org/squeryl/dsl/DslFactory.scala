/*******************************************************************************
 * Copyright 2010 Maxime Lévesque
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************** */
package org.squeryl.dsl

import ast._
import org.squeryl.internals.{OutMapper}
import java.sql.ResultSet
import java.util.Date


trait DslFactory
  extends TypeArithmetic
    with SqlFunctions {

  protected def createLeafNodeOfScalarIntType(i: IntType): NumericalExpression[IntType]
  protected def createLeafNodeOfScalarIntOptionType(i: Option[IntType]): NumericalExpression[Option[IntType]]

  protected def createLeafNodeOfScalarDoubleType(d: DoubleType): NumericalExpression[DoubleType]
  protected def createLeafNodeOfScalarDoubleOptionType(d: Option[DoubleType]): NumericalExpression[Option[DoubleType]]

  protected def createLeafNodeOfScalarBigDecimalType(d: BigDecimalType): NumericalExpression[BigDecimalType]
  protected def createLeafNodeOfScalarBigDecimalOptionType(d: Option[BigDecimalType]): NumericalExpression[Option[BigDecimalType]]

  protected def createLeafNodeOfScalarFloatType(d: FloatType): NumericalExpression[FloatType]
  protected def createLeafNodeOfScalarFloatOptionType(d: Option[FloatType]): NumericalExpression[Option[FloatType]]

  protected def createLeafNodeOfScalarStringType(s: StringType): StringExpression[StringType]
  protected def createLeafNodeOfScalarStringOptionType(s: Option[StringType]): StringExpression[Option[StringType]]

  protected def createLeafNodeOfScalarLongType(s: LongType): NumericalExpression[LongType]
  protected def createLeafNodeOfScalarLongOptionType(s: Option[LongType]): NumericalExpression[Option[LongType]]

  protected def createLeafNodeOfScalarBooleanType(s: BooleanType): BooleanExpression[BooleanType]
  protected def createLeafNodeOfScalarBooleanOptionType(s: Option[BooleanType]): BooleanExpression[Option[BooleanType]]

  protected def createLeafNodeOfScalarBinaryType(s: BinaryType): BinaryExpression[BinaryType]
  protected def createLeafNodeOfScalarBinaryOptionType(s: Option[BinaryType]): BinaryExpression[Option[BinaryType]]

  protected def createLeafNodeOfScalarDateType(d: DateType): DateExpression[DateType]
  protected def createLeafNodeOfScalarDateOptionType(d: Option[DateType]): DateExpression[Option[DateType]]

  protected def createLeafNodeOfScalarTimestampType(d: TimestampType): DateExpression[TimestampType]
  protected def createLeafNodeOfScalarTimestampOptionType(d: Option[TimestampType]): DateExpression[Option[TimestampType]]

  protected def createLeafNodeOfScalarUuidType(d: UuidType): UuidExpression[UuidType]
  protected def createLeafNodeOfScalarUuidOptionType(d: Option[UuidType]): UuidExpression[Option[UuidType]]

  protected def createLeafNodeOfEnumExpressionType[A](e: EnumerationValueType): EnumExpression[EnumerationValueType]
  protected def createLeafNodeOfEnumExpressionOptionType[A](e: Option[EnumerationValueType]): EnumExpression[Option[EnumerationValueType]]

  // expose Factory Methods implicit :
  // ScalarNode Types :
  implicit def int2ScalarInt(i: IntType) = createLeafNodeOfScalarIntType(i)

  implicit def double2ScalarDouble(d: DoubleType) = createLeafNodeOfScalarDoubleType(d)

  implicit def bigDecimal2ScalarBigDecimal(b: BigDecimalType) = createLeafNodeOfScalarBigDecimalType(b)

  implicit def float2ScalarFloat(d: FloatType) = createLeafNodeOfScalarFloatType(d)

  implicit def string2ScalarString(s: StringType) = createLeafNodeOfScalarStringType(s)

  implicit def long2ScalarLong(l: LongType) = createLeafNodeOfScalarLongType(l)

  implicit def bool2ScalarBoolean(b: BooleanType) = createLeafNodeOfScalarBooleanType(b)

  implicit def date2ScalarDate(b: DateType) = createLeafNodeOfScalarDateType(b)

  implicit def optionInt2ScalarInt(i: Option[IntType]) = createLeafNodeOfScalarIntOptionType(i)

  implicit def optionLong2ScalarLong(i: Option[LongType]) = createLeafNodeOfScalarLongOptionType(i)

  implicit def optionString2ScalarString(i: Option[StringType]) = createLeafNodeOfScalarStringOptionType(i)

  implicit def optionDouble2ScalarDouble(i: Option[DoubleType]) = createLeafNodeOfScalarDoubleOptionType(i)

  implicit def optionBigDecimal2ScalarBigDecimal(i: Option[BigDecimalType]) = createLeafNodeOfScalarBigDecimalOptionType(i)

  implicit def optionFloat2ScalarFloat(i: Option[FloatType]) = createLeafNodeOfScalarFloatOptionType(i)

  implicit def optionBoolean2ScalarBoolean(i: Option[BooleanType]) = createLeafNodeOfScalarBooleanOptionType(i)

  implicit def optionDate2ScalarDate(i: Option[DateType]) = createLeafNodeOfScalarDateOptionType(i)

  implicit def timestamp2ScalarTimestamp(ts: TimestampType) = createLeafNodeOfScalarTimestampType(ts)

  implicit def timestamp2ScalarTimestampOptionNode(ts: Option[TimestampType]) = createLeafNodeOfScalarTimestampOptionType(ts)

  implicit def enum2EnumNode[A <: EnumerationValueType](e: A): EnumExpression[A] =
    createLeafNodeOfEnumExpressionType(e).asInstanceOf[EnumExpression[A]]

  implicit def enum2OptionEnumNode[A <: Option[EnumerationValueType]](e: A): EnumExpression[Option[A]] =
    createLeafNodeOfEnumExpressionOptionType(e).asInstanceOf[EnumExpression[Option[A]]]

  implicit def uuid2ScalarUuid(u: UuidType) = createLeafNodeOfScalarUuidType(u)

  implicit def optionUuid2ScalarUuid(u: Option[UuidType]) = createLeafNodeOfScalarUuidOptionType(u)

  implicit def binary2ScalarBinary(b: BinaryType) = createLeafNodeOfScalarBinaryType(b)

  implicit def binaryOption2ScalarBinaryOption(b: Option[BinaryType]) = createLeafNodeOfScalarBinaryOptionType(b)
  
  // List Conversion implicits don't vary with the choice of
  // column/field types, so they don't need to be overridable factory methods :

  implicit def traversableOfNumericalExpressionList[A <% NumericalExpression[_]](l: Traversable[A]) =
    new RightHandSideOfIn[NumericalExpression[A]](new ConstantExpressionNodeList[Any](l))

  implicit def traversableOfEnumerationValue2ListEnumerationValue[E <: Enumeration#Value](l: Traversable[E]) = 
    new RightHandSideOfIn[E](new ConstantExpressionNodeList[E](l)) 

// TODO : find out why this generalized conv for NonNumericals won't work (looks like a scalac bug...):
//  implicit def traversableOfNonNumercalExpressionList[A <% NonNumericalExpression[_]](l: Traversable[A]) =
//    new RightHandSideOfIn[NonNumericalExpression[A]](new ConstantExpressionNodeList[Any](l))

  implicit def traversableOfString2ListString(l: Traversable[StringType]) =
    new RightHandSideOfIn[StringType](new ConstantExpressionNodeList[StringType](l))

  implicit def traversableOfUuid2ListUuid(l: Traversable[UuidType]) =
    new RightHandSideOfIn[UuidType](new ConstantExpressionNodeList[UuidType](l))

  implicit def traversableOfString2OptionListString(l: Traversable[StringType]) =
    new RightHandSideOfIn[Option[StringType]](new ConstantExpressionNodeList[StringType](l))

  implicit def traversableOfDate2ListDate(l: Traversable[DateType]) =
    new RightHandSideOfIn[DateType](new ConstantExpressionNodeList[DateType](l))

  implicit def traversableOfDate2OptionListDate(l: Traversable[DateType]) =
    new RightHandSideOfIn[Option[DateType]](new ConstantExpressionNodeList[DateType](l))

  implicit def traversableOfUuidOptionList(l: Traversable[UuidType]) =
    new RightHandSideOfIn[Option[UuidType]](new ConstantExpressionNodeList[UuidType](l))

  implicit def typedExpression2OrderByArg[E <% TypedExpressionNode[_]](e: E) = new OrderByArg(e)

  implicit def orderByArg2OrderByExpression(a: OrderByArg) = new OrderByExpression(a)

}
