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
package org.squeryl.internals

import org.squeryl.dsl.ast._
import org.squeryl._
import dsl.CompositeKey
import org.squeryl.{Schema, Session, Table}
import java.sql._
import java.util.UUID

trait DatabaseAdapter {

  class Zip[T](val element: T, val isLast: Boolean, val isFirst: Boolean)
  
  class ZipIterable[T](iterable: Iterable[T]) {
    val count = iterable.size
    def foreach[U](f: Zip[T] => U):Unit = {
      var c = 1  
      for(i <- iterable) {
        f(new Zip(i, c == count, c == 1))
        c += 1
      }
    }

    def zipi = this
  }

  implicit def zipIterable[T](i: Iterable[T]) = new ZipIterable(i)

  def writeQuery(qen: QueryExpressionElements, sw: StatementWriter):Unit =
    writeQuery(qen, sw, false, None)

  protected def writeQuery(qen: QueryExpressionElements, sw: StatementWriter, inverseOrderBy: Boolean, topHint: Option[String]):Unit = {

    sw.write("Select")

    topHint.foreach(" "  + sw.write(_) + " ")

    if(qen.selectDistinct)
      sw.write(" distinct")
    
    sw.nextLine
    sw.writeIndented {
      sw.writeNodesWithSeparator(qen.selectList.filter(e => ! e.inhibited), ",", true)
    }
    sw.nextLine
    sw.write("From")
    sw.nextLine

    if(!qen.isJoinForm) {
      sw.writeIndented {
        for(z <- qen.tableExpressions.zipi) {
          z.element.write(sw)
          sw.write(" ")
          sw.write(sw.quoteName(z.element.alias))
          if(!z.isLast) {
            sw.write(",")
            sw.nextLine
          }
        }
        sw.pushPendingNextLine
      }
    }
    else {
      val singleNonJoinTableExpression = qen.tableExpressions.filter(! _.isMemberOfJoinList)
      assert(singleNonJoinTableExpression.size == 1, "join query must have exactly one FROM argument, got : " + qen.tableExpressions)
      val firstJoinExpr = singleNonJoinTableExpression.head
      val restOfJoinExpr = qen.tableExpressions.filter(_.isMemberOfJoinList)
      firstJoinExpr.write(sw)
      sw.write(" ")
      sw.write(sw.quoteName(firstJoinExpr.alias))
      sw.nextLine

      for(z <- restOfJoinExpr.zipi) {
        writeJoin(z.element, sw)
        if(z.isLast)
          sw.unindent
        sw.pushPendingNextLine
      }
    }

    writeEndOfFromHint(qen, sw)
    
    if(qen.hasUnInhibitedWhereClause) {      
      sw.write("Where")
      sw.nextLine
      sw.writeIndented {
        qen.whereClause.get.write(sw)
      }
      sw.pushPendingNextLine
    }

    if(! qen.groupByClause.isEmpty) {      
      sw.write("Group By")
      sw.nextLine
      sw.writeIndented {
        sw.writeNodesWithSeparator(qen.groupByClause.filter(e => ! e.inhibited), ",", true)
      }
      sw.pushPendingNextLine
    }

    if(! qen.havingClause.isEmpty) {
      sw.write("Having")
      sw.nextLine
      sw.writeIndented {
        sw.writeNodesWithSeparator(qen.havingClause.filter(e => ! e.inhibited), ",", true)
      }
      sw.pushPendingNextLine
    }

    if(! qen.orderByClause.isEmpty && qen.parent == None) {
      sw.write("Order By")
      sw.nextLine
      val ob0 = qen.orderByClause.filter(e => ! e.inhibited)
      val ob = if(inverseOrderBy) ob0.map(_.asInstanceOf[OrderByExpression].inverse) else ob0
      sw.writeIndented {
        sw.writeNodesWithSeparator(ob, ",", true)
      }
      sw.pushPendingNextLine
    }

    writeEndOfQueryHint(qen, sw)

    writePaginatedQueryDeclaration(qen, sw)
  }

  def writeEndOfQueryHint(qen: QueryExpressionElements, sw: StatementWriter) = 
    if(qen.isForUpdate) {
      sw.write("for update")
      sw.pushPendingNextLine
    }

  def writeEndOfFromHint(qen: QueryExpressionElements, sw: StatementWriter) = {}

  def writePaginatedQueryDeclaration(qen: QueryExpressionElements, sw: StatementWriter):Unit = 
    qen.page.foreach(p => {
      sw.write("limit ")
      sw.write(p._2.toString)
      sw.write(" offset ")
      sw.write(p._1.toString)
    })


  def writeJoin(queryableExpressionNode: QueryableExpressionNode, sw: StatementWriter) = {
    sw.write(queryableExpressionNode.joinKind.get._1)
    sw.write(" ")
    sw.write(queryableExpressionNode.joinKind.get._2)
    sw.write(" join ")
    queryableExpressionNode.write(sw)
    sw.write(" as ")
    sw.write(sw.quoteName(queryableExpressionNode.alias))
    sw.write(" on ")
    queryableExpressionNode.joinExpression.get.write(sw)
  }

  def intTypeDeclaration = "int"
  def stringTypeDeclaration = "varchar"
  def stringTypeDeclaration(length:Int) = "varchar("+length+")"
  def booleanTypeDeclaration = "boolean"
  def doubleTypeDeclaration = "double"
  def dateTypeDeclaration = "date"
  def longTypeDeclaration = "bigint"
  def floatTypeDeclaration = "real"
  def bigDecimalTypeDeclaration = "decimal"
  def bigDecimalTypeDeclaration(precision:Int, scale:Int) = "decimal(" + precision + "," + scale + ")"
  def timestampTypeDeclaration = "timestamp"
  def binaryTypeDeclaration = "binary"
  def uuidTypeDeclaration = "char(36)"

  private val _declarationHandler = new FieldTypeHandler[String] {

    def handleIntType = intTypeDeclaration
    def handleStringType  = stringTypeDeclaration
    def handleStringType(fmd: Option[FieldMetaData]) =
      fmd match {
        case Some(x) => stringTypeDeclaration(x.length)
        case None => stringTypeDeclaration
      }

    def handleBooleanType = booleanTypeDeclaration
    def handleDoubleType = doubleTypeDeclaration
    def handleDateType = dateTypeDeclaration
    def handleLongType = longTypeDeclaration
    def handleFloatType = floatTypeDeclaration
    def handleBigDecimalType(fmd: Option[FieldMetaData]) =
      fmd match {
        case Some(x) => bigDecimalTypeDeclaration(x.length, x.scale)
        case None => bigDecimalTypeDeclaration
      }

    def handleTimestampType = timestampTypeDeclaration
    def handleBinaryType = binaryTypeDeclaration
    def handleUuidType = uuidTypeDeclaration
    def handleEnumerationValueType = intTypeDeclaration
    def handleUnknownType(c: Class[_]) =
      org.squeryl.internals.Utils.throwError("don't know how to map field type " + c.getName)
  }
  
  def databaseTypeFor(fmd: FieldMetaData) =
    fmd.explicitDbTypeDeclaration.getOrElse(
      fmd.schema.columnTypeFor(fmd, fmd.parentMetaData.viewOrTable.asInstanceOf[Table[_]]).getOrElse(
        _declarationHandler.handleType(fmd.wrappedFieldType, Some(fmd))
      )
    )

  def writeColumnDeclaration(fmd: FieldMetaData, isPrimaryKey: Boolean, schema: Schema): String = {

    val dbTypeDeclaration = databaseTypeFor(fmd)

    val sb = new StringBuilder(128)
  
    sb.append("  ")
    sb.append(quoteName(fmd.columnName))
    sb.append(" ")
    sb.append(dbTypeDeclaration)

    for(d <- fmd.defaultValue) {
      sb.append(" default ")

      val v = convertToJdbcValue(d.value.asInstanceOf[AnyRef])
      if(v.isInstanceOf[String])
        sb.append("'" + v + "'")
      else
        sb.append(v)
    }

    if(isPrimaryKey)
      sb.append(" primary key")

    if(!fmd.isOption)
      sb.append(" not null")
    
    if(supportsAutoIncrementInColumnDeclaration && fmd.isAutoIncremented)
      sb.append(" auto_increment")

    sb.toString
  }

  def supportsAutoIncrementInColumnDeclaration:Boolean = true

  def writeCreateTable[T](t: Table[T], sw: StatementWriter, schema: Schema) = {

    sw.write("create table ")
    sw.write(quoteName(t.prefixedName))
    sw.write(" (\n");
    sw.writeIndented {
      sw.writeLinesWithSeparator(
        t.posoMetaData.fieldsMetaData.map(
          fmd => writeColumnDeclaration(fmd, fmd.declaredAsPrimaryKeyInSchema, schema)
        ),
        ","
      )
    }
    sw.write(")")
  }                     
  
  def convertParamsForJdbc(params: Iterable[AnyRef]) =
    for(p <- params) yield {
       p match {
         case null => null	        
	     case None => null
	     case Some(x: AnyRef) => convertToJdbcValue(x)
	     case x: AnyRef =>  convertToJdbcValue(x)
	   }     
    }
        
  def fillParamsInto(params: Iterable[AnyRef], s: PreparedStatement) {    
    var i = 1;
    for(p <- params) {
      s.setObject(i, p)
      i += 1
    }    
  }

  private def _exec[A](s: Session, sw: StatementWriter, block: Iterable[AnyRef]=>A, args: Iterable[AnyRef]): A =
    try {
      if(s.isLoggingEnabled)
        s.log(sw.toString)      
      block(args)
    }
    catch {
      case e: SQLException =>
          throw new SquerylException(
            "Exception while executing statement : "+ e.getMessage+
           "\nerrorCode: " +
            e.getErrorCode + ", sqlState: " + e.getSQLState + "\n" +
            sw.statement + "\njdbcParams:" + 
            args.mkString("[",",","]"), e)
    }    

  def failureOfStatementRequiresRollback = false

  /**
   * Some methods like 'dropTable' issue their statement, and will silence the exception.
   * For example dropTable will silence when isTableDoesNotExistException(theExceptionThrown).
   * It must be used carefully, and an exception should not be silenced unless identified.
   */
  protected def execFailSafeExecute(sw: StatementWriter, silenceException: SQLException => Boolean): Unit = {
    val s = Session.currentSession
    val c = s.connection
    val stat = c.createStatement
    val sp =
      if(failureOfStatementRequiresRollback) Some(c.setSavepoint)
      else None

    try {
      if(s.isLoggingEnabled)
        s.log(sw.toString)
      stat.execute(sw.statement)
    }
    catch {
      case e: SQLException =>
        if(silenceException(e))
          sp.foreach(c.rollback(_))
        else
          throw new SquerylException(
            "Exception while executing statement,\n" +
            "SQLState:" + e.getSQLState + ", ErrorCode:" + e.getErrorCode + "\n :" +
            sw.statement, e)
    }
    finally {
      sp.foreach(c.releaseSavepoint(_))
      Utils.close(stat)
    }
  }
  
  implicit def string2StatementWriter(s: String) = {
    val sw = new StatementWriter(this)
    sw.write(s)
    sw
  }

  protected def exec[A](s: Session, sw: StatementWriter)(block: Iterable[AnyRef]=>A): A = {
    val p = convertParamsForJdbc(sw.paramsZ)
    _exec[A](s, sw, block, p)
  }

  def executeQuery(s: Session, sw: StatementWriter) = exec(s, sw) { params =>
    val st = s.connection.prepareStatement(sw.statement)
    fillParamsInto(params, st)
    (st.executeQuery, st)
  }

  def executeUpdate(s: Session, sw: StatementWriter):(Int,PreparedStatement) = exec(s, sw) { params =>
    val st = s.connection.prepareStatement(sw.statement)
    fillParamsInto(params, st)
    (st.executeUpdate, st)
  }

  def executeUpdateAndCloseStatement(s: Session, sw: StatementWriter): Int = exec(s, sw) { params =>
    val st = s.connection.prepareStatement(sw.statement)
    fillParamsInto(params, st)
    try {
      st.executeUpdate
    }
    finally {
      st.close
    }
  }

  def executeUpdateForInsert(s: Session, sw: StatementWriter, ps: PreparedStatement) = exec(s, sw) { params =>
    fillParamsInto(params, ps)
    ps.executeUpdate
  }

  protected def getInsertableFields(fmd : Iterable[FieldMetaData]) = fmd.filter(fmd => !fmd.isAutoIncremented && fmd.isInsertable )

  def writeInsert[T](o: T, t: Table[T], sw: StatementWriter):Unit = {

    val o_ = o.asInstanceOf[AnyRef]    
    val f = getInsertableFields(t.posoMetaData.fieldsMetaData)

    sw.write("insert into ");
    sw.write(quoteName(t.prefixedName));
    sw.write(" (");
    sw.write(f.map(fmd => quoteName(fmd.columnName)).mkString(", "));
    sw.write(") values ");
    sw.write(
      f.map(fmd => writeValue(o_, fmd, sw)
    ).mkString("(",",",")"));
  }

  /**
   * Converts field instances so they can be fed, and understood by JDBC
   * will not do conversion from None/Some, so @arg r should be a java primitive type or
   * a CustomType
   */
  def convertToJdbcValue(r: AnyRef) : AnyRef = {
    var v = r
    if(v.isInstanceOf[Product1[_]])
       v = v.asInstanceOf[Product1[Any]]._1.asInstanceOf[AnyRef]

    if(v.isInstanceOf[java.util.Date] && ! v.isInstanceOf[java.sql.Date]  && ! v.isInstanceOf[Timestamp])
       v = new java.sql.Timestamp(v.asInstanceOf[java.util.Date].getTime)
    else if(v.isInstanceOf[scala.math.BigDecimal])
       v = v.asInstanceOf[scala.math.BigDecimal].bigDecimal
    else if(v.isInstanceOf[scala.Enumeration#Value])
       v = v.asInstanceOf[scala.Enumeration#Value].id.asInstanceOf[AnyRef]
    else if(v.isInstanceOf[java.util.UUID])
       v = convertFromUuidForJdbc(v.asInstanceOf[UUID])
    v
  }

//  see comment in def convertFromBooleanForJdbc
//    if(v.isInstanceOf[java.lang.Boolean])
//      v = convertFromBooleanForJdbc(v)
  
  // TODO: move to StatementWriter (since it encapsulates the 'magic' of swapping values for '?' when needed)
  //and consider delaying the ? to 'value' decision until execution, in order to make StatementWriter loggable
  //with values at any time (via : a kind of prettyStatement method)
  protected def writeValue(o: AnyRef, fmd: FieldMetaData, sw: StatementWriter):String =
    if(sw.isForDisplay) {
      val v = fmd.get(o)
      if(v != null)
        v.toString
      else
        "null"
    }
    else {
      sw.addParam(convertToJdbcValue(fmd.get(o)))
      "?"
    }

//  protected def writeValue(sw: StatementWriter, v: AnyRef):String =
//    if(sw.isForDisplay) {
//      if(v != null)
//        v.toString
//      else
//        "null"
//    }
//    else {
//      sw.addParam(convertToJdbcValue(v))
//      "?"
//    }

  /**
   * When @arg printSinkWhenWriteOnlyMode is not None, the adapter will not execute any statement, but only silently give it to the String=>Unit closure
   */
  def postCreateTable(t: Table[_], printSinkWhenWriteOnlyMode: Option[String => Unit]) = {}
  
  def postDropTable(t: Table[_]) = {}

  def createSequenceName(fmd: FieldMetaData) = 
    "s_" + fmd.parentMetaData.viewOrTable.name + "_" + fmd.columnName

  def writeConcatFunctionCall(fn: FunctionNode[_], sw: StatementWriter) = {
    sw.write(fn.name)
    sw.write("(")
    sw.writeNodesWithSeparator(fn.args, ",", false)
    sw.write(")")    
  }

  def isFullOuterJoinSupported = true

  def writeUpdate[T](o: T, t: Table[T], sw: StatementWriter, checkOCC: Boolean) = {

    val o_ = o.asInstanceOf[AnyRef]


    sw.write("update ", quoteName(t.prefixedName), " set ")
    sw.nextLine
    sw.indent
    sw.writeLinesWithSeparator(
      t.posoMetaData.fieldsMetaData.
        filter(fmd=> ! fmd.isIdFieldOfKeyedEntity && fmd.isUpdatable).
          map(fmd => {
            if(fmd.isOptimisticCounter)
              quoteName(fmd.columnName) + " = " + quoteName(fmd.columnName) + " + 1 "
            else
              quoteName(fmd.columnName) + " = " + writeValue(o_, fmd, sw)
          }),
      ","
    )
    sw.unindent
    sw.write("where")
    sw.nextLine
    sw.indent
    
    t.posoMetaData.primaryKey.getOrElse(org.squeryl.internals.Utils.throwError("writeUpdate was called on an object that does not extend from KeyedEntity[]")).fold(
      pkMd => sw.write(quoteName(pkMd.columnName), " = ", writeValue(o_, pkMd, sw)),
      pkGetter => {
        val astOfQuery4WhereClause = Utils.createQuery4WhereClause(t, (t0:T) =>
          pkGetter.invoke(t0).asInstanceOf[CompositeKey].buildEquality(o.asInstanceOf[KeyedEntity[CompositeKey]].id))

        astOfQuery4WhereClause.inhibitAliasOnSelectElementReference = true
        astOfQuery4WhereClause.whereClause.get.write(sw)
      }
    )

    if(checkOCC)
      t.posoMetaData.optimisticCounter.foreach(occ => {
         sw.write(" and ")
         sw.write(quoteName(occ.columnName))
         sw.write(" = ")
         sw.write(writeValue(o_, occ, sw))
      })
  }

  def writeDelete[T](t: Table[T], whereClause: Option[ExpressionNode], sw: StatementWriter) = {

    sw.write("delete from ")
    sw.write(quoteName(t.prefixedName))
    if(whereClause != None) {
      sw.nextLine
      sw.write("where")
      sw.nextLine
      sw.writeIndented {
        whereClause.get.write(sw)
      }
    }
  }

  /**
   * unused at the moment, since all jdbc drivers adhere to the standard that :
   *  1 == true, false otherwise. If a new driver would not adhere
   * to this, the call can be uncommented in method convertToJdbcValue
   */
  def convertFromBooleanForJdbc(b: Boolean): Boolean = b

  /**
   * unused for the same reason as def convertFromBooleanForJdbc (see comment)
   */
  def convertToBooleanForJdbc(rs: ResultSet, i:Int): Boolean = rs.getBoolean(i)

  def convertFromUuidForJdbc(u: UUID): AnyRef =
    u.toString

  def convertToUuidForJdbc(rs: ResultSet, i:Int): UUID =
    UUID.fromString(rs.getString(i))

  def writeUpdate(t: Table[_], us: UpdateStatement, sw : StatementWriter) = {

    val colsToUpdate = us.columns.iterator

    sw.write("update ")
    sw.write(quoteName(t.prefixedName))
    sw.write(" set")
    sw.indent
    sw.nextLine
    for(z <- us.values.zipi) {
      val col = colsToUpdate.next
      sw.write(quoteName(col.columnName))
      sw.write(" = ")
      val v = z.element
      sw.write("(")
      v.write(sw)
      sw.write(")")
      if(!z.isLast) {
        sw.write(",")
        sw.nextLine
      }      
    }

    if(t.posoMetaData.isOptimistic) {
      sw.write(",")
      sw.nextLine      
      val occ = t.posoMetaData.optimisticCounter.get
      sw.write(quoteName(occ.columnName))
      sw.write(" = ")
      sw.write(quoteName(occ.columnName) + " + 1")
    }
    
    sw.unindent

    if(us.whereClause != None) {
      sw.nextLine
      sw.write("Where")
      sw.nextLine
      sw.writeIndented {
        us.whereClause.get.write(sw)
      }
    }
  }

  def nvlToken = "coalesce"

  def writeNvlCall(left: ExpressionNode, right: ExpressionNode, sw: StatementWriter) = {
    sw.write(nvlToken)
    sw.write("(")
    left.write(sw)
    sw.write(",")
    right.write(sw)
    sw.write(")")
  }

  /**
   * Figures out from the SQLException (ex.: vendor specific error code) 
   * if it's cause is a NOT NULL constraint violation
   */
  def isNotNullConstraintViolation(e: SQLException): Boolean = false  

  def foreignKeyConstraintName(foreignKeyTable: Table[_], idWithinSchema: Int) =
    foreignKeyTable.name + "FK" + idWithinSchema

  def viewAlias(vn: ViewExpressionNode[_]) =
     if(vn.view.prefix != None)
       vn.view.prefix.get + "_" + vn.view.name + vn.uniqueId.get
     else
       vn.view.name + vn.uniqueId.get

  def writeForeignKeyDeclaration(
    foreignKeyTable: Table[_], foreignKeyColumnName: String,
    primaryKeyTable: Table[_], primaryKeyColumnName: String,
    referentialAction1: Option[ReferentialAction],
    referentialAction2: Option[ReferentialAction],
    fkId: Int) = {
    
    val sb = new StringBuilder(256)

    sb.append("alter table ")
    sb.append(quoteName(foreignKeyTable.prefixedName))
    sb.append(" add constraint ")
    sb.append(quoteName(foreignKeyConstraintName(foreignKeyTable, fkId)))
    sb.append(" foreign key (")
    sb.append(quoteName(foreignKeyColumnName))
    sb.append(") references ")
    sb.append(quoteName(primaryKeyTable.prefixedName))
    sb.append("(")
    sb.append(quoteName(primaryKeyColumnName))
    sb.append(")")

    val f =  (ra:ReferentialAction) => {
      sb.append(" on ")
      sb.append(ra.event)
      sb.append(" ")
      sb.append(ra.action)
    }

    referentialAction1.foreach(f)
    referentialAction2.foreach(f)

    sb.toString
  }

  protected def currenSession =
    Session.currentSession

  def writeDropForeignKeyStatement(foreignKeyTable: Table[_], fkName: String) =
    "alter table " + quoteName(foreignKeyTable.prefixedName) + " drop constraint " + quoteName(fkName)

  def dropForeignKeyStatement(foreignKeyTable: Table[_], fkName: String, session: Session):Unit =
    execFailSafeExecute(writeDropForeignKeyStatement(foreignKeyTable, fkName), e => true)

  def isTableDoesNotExistException(e: SQLException): Boolean

  def supportsForeignKeyConstraints = true

  def writeDropTable(tableName: String) =
    "drop table " + quoteName(tableName)

  def dropTable(t: Table[_]) =
    execFailSafeExecute(writeDropTable(t.prefixedName), e=> isTableDoesNotExistException(e))

  def writeUniquenessConstraint(t: Table[_], cols: Iterable[FieldMetaData]) = {
    //ALTER TABLE TEST ADD CONSTRAINT NAME_UNIQUE UNIQUE(NAME)
    val sb = new StringBuilder(256)
    
    sb.append("alter table ")
    sb.append(quoteName(t.prefixedName))
    sb.append(" add constraint ")
    sb.append(quoteName(t.prefixedName + "CPK"))
    sb.append(" unique(")
    sb.append(cols.map(_.columnName).map(quoteName(_)).mkString(","))
    sb.append(")")
    sb.toString
  }


  def writeRegexExpression(left: ExpressionNode, pattern: String, sw: StatementWriter) = {
    sw.write("(")
    left.write(sw)
    sw.write(" ~ ?)")
    sw.addParam(pattern)
  }

  def writeConcatOperator(left: ExpressionNode, right: ExpressionNode, sw: StatementWriter) = {
    val binaryOpNode = new BinaryOperatorNode(left, right, "||")
    binaryOpNode.doWrite(sw)
  }

//  /**
//   * @nameOfCompositeKey when not None, the column group forms a composite key, 'nameOfCompositeKey' can be used
//   * as part of the name to create a more meaningfull name for the constraint
//   */
//  def writeUniquenessConstraint(columnDefs: Seq[FieldMetaData], nameOfCompositeKey: Option[String]) = ""

  /**
   * @name the name specified in the Schema, when not None, it  must be used as the name
   * @nameOfCompositeKey when not None, the column group forms a composite key, 'nameOfCompositeKey' can be used
   * as part of the name to create a more meaningfull name for the constraint, when 'name' is None
   */
  def writeIndexDeclaration(columnDefs: Seq[FieldMetaData], name:Option[String], nameOfCompositeKey: Option[String], isUnique: Boolean) = {                                    
    val sb = new StringBuilder(256)
    sb.append("create ")

    if(isUnique)
      sb.append("unique ")

    sb.append("index ")

    val tableName = columnDefs.head.parentMetaData.viewOrTable.prefixedName

    if(name != None)
      sb.append(quoteName(name.get))
    else if(nameOfCompositeKey != None)
      sb.append(quoteName("idx" + nameOfCompositeKey.get))
    else
      sb.append(quoteName("idx" + generateAlmostUniqueSuffixWithHash(tableName + "-" + columnDefs.map(_.columnName).mkString("-"))))

    sb.append(" on ")

    sb.append(quoteName(tableName))

    sb.append(columnDefs.map(_.columnName).map(quoteName(_)).mkString(" (",",",")"))

    sb.toString
  }

  /**
   * This will create an probabilistically unique string of length no longer than 11 chars,
   * it can be used to create "almost unique" names where uniqueness is not an absolute requirement,
   * is not ,
   */
  def generateAlmostUniqueSuffixWithHash(s: String): String = {
    val a32 = new java.util.zip.Adler32
    a32.update(s.getBytes)
    a32.getValue.toHexString
  }

  def quoteIdentifier(s: String) = s

  def quoteName(s: String) = s.split('.').map(quoteIdentifier(_)).mkString(".")

  def fieldAlias(n: QueryableExpressionNode, fse: FieldSelectElement) =
    n.alias + "_" + fse.fieldMetaData.columnName

  def aliasExport(parentOfTarget: QueryableExpressionNode, target: SelectElement) =
    parentOfTarget.alias + "_" + target.aliasSegment

  def writeSelectElementAlias(se: SelectElement, sw: StatementWriter) = {
    val a = se.aliasSegment
//    if(a.length > 30)
//      org.squeryl.internals.Utils.throwError("Oracle Bust : " + a)
    sw.write(quoteName(a))
  }

  def databaseTypeFor(c: Class[_]) =
    _declarationHandler.handleType(c, None)

  def writeCastInvocation(e: TypedExpressionNode[_], sw: StatementWriter) = {
    sw.write("cast(")
    e.write(sw)

    val dbSpecificType = databaseTypeFor(e.mapper.jdbcClass)

    sw.write(" as ")
    sw.write(dbSpecificType)
    sw.write(")")
  }

  def writeCaseStatement(toMatch: Option[ExpressionNode], cases: Iterable[(ExpressionNode, TypedExpressionNode[_])], otherwise: TypedExpressionNode[_], sw: StatementWriter) = {

    sw.write("(case ")
    toMatch.foreach(_.write(sw))
    sw.indent
    sw.nextLine

    for(c <- cases) {
      sw.write("when ")
      c._1.write(sw)
      sw.write(" then ")
      writeCastInvocation(c._2, sw)
      sw.nextLine
    }

    sw.write("else ")
    writeCastInvocation(otherwise,sw)
    sw.nextLine
    sw.unindent
    sw.write("end)")
  }
}
