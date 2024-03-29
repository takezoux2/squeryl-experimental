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
package org.squeryl

import dsl._
import ast._
import internals._
import reflect.{Manifest}
import java.sql.SQLException
import java.io.PrintWriter
import collection.mutable.{HashMap, HashSet, ArrayBuffer}

trait Schema {

  protected implicit def thisSchema = this

  /**
   * Contains all Table[_]s in this shema, and also all ManyToManyRelation[_,_,_]s (since they are also Table[_]s
   */
  private val _tables = new ArrayBuffer[Table[_]] 
  
  private val _tableTypes = new HashMap[Class[_], Table[_]]

  private val _oneToManyRelations = new ArrayBuffer[OneToManyRelation[_,_]]

  private val _manyToManyRelations = new ArrayBuffer[ManyToManyRelation[_,_,_]]

  private val _columnGroupAttributeAssignments = new ArrayBuffer[ColumnGroupAttributeAssignment]

  private [squeryl] val _namingScope = new HashSet[String] 

  private [squeryl] def _addRelation(r: OneToManyRelation[_,_]) =
    _oneToManyRelations.append(r)

  private [squeryl] def _addRelation(r: ManyToManyRelation[_,_,_]) =
    _manyToManyRelations.append(r)

  private def _dbAdapter = Session.currentSession.databaseAdapter

  /**
   *  @returns a tuple of (Table[_], Table[_], ForeignKeyDeclaration) where
   *  ._1 is the foreign key table,
   *  ._2 is the primary key table
   *  ._3 is the ForeignKeyDeclaration between _1 and _2
   */
  private def _activeForeignKeySpecs = {
    val res = new ArrayBuffer[(Table[_], Table[_], ForeignKeyDeclaration)]

    for( r <- _oneToManyRelations if r.foreignKeyDeclaration._isActive)
      res.append((r.rightTable, r.leftTable, r.foreignKeyDeclaration))

    for(r <- _manyToManyRelations) {
      if(r.leftForeignKeyDeclaration._isActive)
        res.append((r.thisTable, r.leftTable , r.leftForeignKeyDeclaration))
      if(r.rightForeignKeyDeclaration._isActive)
        res.append((r.thisTable, r.rightTable, r.rightForeignKeyDeclaration))
    }

    res
  }


  def findTablesFor[A](a: A): Iterable[Table[A]] = {
    val c = a.asInstanceOf[AnyRef].getClass
    _tables.filter(_.posoMetaData.clasz == c).asInstanceOf[Iterable[Table[A]]]
  }

  private [squeryl] def findAllTablesFor[A](c: Class[A]) =
    _tables.filter(t => c.isAssignableFrom(t.posoMetaData.clasz)).asInstanceOf[Traversable[Table[_]]]


  object NamingConventionTransforms {
    
    @deprecated("use snakify() instead as of 0.9.5beta")
    def camelCase2underScore(name: String) =
      name.toList.map(c => if(c.isUpper) "_" + c else c).mkString
      
    def snakify(name: String) =
      name.replaceAll("^([^A-Za-z_])", "_$1").replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase
  }

  def columnNameFromPropertyName(propertyName: String) = propertyName

  def tableNameFromClassName(tableName: String) = tableName

  def name: Option[String] = None

  /**
   * Prints the schema to standard output, it is simply : schema.printDdl(println(_))
   */
  def printDdl: Unit = printDdl(println(_))

  def printDdl(pw: PrintWriter): Unit = printDdl(pw.println(_))

  /**
   * @arg statementHandler is a closure that receives every declaration in the schema.
   */
  def printDdl(statementHandler: String => Unit): Unit = {

    statementHandler("-- table declarations :")

    for(t <- _tables) {
      val sw = new StatementWriter(true, _dbAdapter)
      _dbAdapter.writeCreateTable(t, sw, this)
      statementHandler(sw.statement + ";")
      _dbAdapter.postCreateTable(t, Some(statementHandler))

      val indexDecl = _indexDeclarationsFor(t)

      if(indexDecl != Nil)
        statementHandler("-- indexes on " + t.prefixedName)

      for(i <- indexDecl)
        statementHandler(i + ";")
    }

    val constraints = _foreignKeyConstraints.toList
    
    if(constraints != Nil)
      statementHandler("-- foreign key constraints :")

    for(fkc <- constraints)
      statementHandler(fkc + ";")

    val compositePKs = _allCompositePrimaryKeys.toList

    if(compositePKs != Nil)
      statementHandler("-- composite key indexes :")
    
    for(cpk <- compositePKs) {
      val createConstraintStmt = _dbAdapter.writeUniquenessConstraint(cpk._1, cpk._2)
      statementHandler(createConstraintStmt + ";")
    }

    val columnGroupIndexes = _writeColumnGroupAttributeAssignments.toList

    if(columnGroupIndexes != Nil)
      statementHandler("-- column group indexes :")

    for(decl <- columnGroupIndexes)
      statementHandler(decl + ";")
  }

  /**
   * This will drop all tables and related sequences in the schema... it's a
   * dangerous operation, typically this is only useful for development
   * database instances, the method is protected in order to make it a little
   * less 'accessible'
   */
  def drop: Unit = {

    if(_dbAdapter.supportsForeignKeyConstraints)
      _dropForeignKeyConstraints

    val s = Session.currentSession.connection.createStatement
    val con = Session.currentSession.connection

    for(t <- _tables) {
      _dbAdapter.dropTable(t)
      _dbAdapter.postDropTable(t)
    }
  }  

  def create = {
    _createTables
    if(_dbAdapter.supportsForeignKeyConstraints)
      _declareForeignKeyConstraints

    _createUniqueConstraintsOfCompositePKs

    createColumnGroupConstraintsAndIndexes
  }

  private def _indexDeclarationsFor(t: Table[_]) = {
    val d0 =
      for(fmd <- t.posoMetaData.fieldsMetaData)
        yield _writeIndexDeclarationIfApplicable(fmd.columnAttributes.toSeq, Seq(fmd), None)

    d0.filter(_ != None).map(_.get).toList
  }
  

  private def _writeColumnGroupAttributeAssignments: Seq[String] =
    for(cgaa <- _columnGroupAttributeAssignments)
      yield _writeIndexDeclarationIfApplicable(cgaa.columnAttributes, cgaa.columns, cgaa.name).
        getOrElse(org.squeryl.internals.Utils.throwError("emtpy attribute list should not be possible to create with DSL (Squeryl bug)."))

  private def _writeIndexDeclarationIfApplicable(columnAttributes: Seq[ColumnAttribute], cols: Seq[FieldMetaData], name: Option[String]): Option[String] = {

    val unique = columnAttributes.find(_.isInstanceOf[Unique])
    val indexed = columnAttributes.find(_.isInstanceOf[Indexed])
  
    (unique, indexed) match {
      case (None,    None)                   => None
      case (Some(_), None)                   => Some(_dbAdapter.writeIndexDeclaration(cols, None,    name, true))
      case (None,    Some(Indexed(idxName))) => Some(_dbAdapter.writeIndexDeclaration(cols, idxName, name, false))
      case (Some(_), Some(Indexed(idxName))) => Some(_dbAdapter.writeIndexDeclaration(cols, idxName, name, true))
    }
  }
  
  def createColumnGroupConstraintsAndIndexes =
    for(statement <- _writeColumnGroupAttributeAssignments)
      _executeDdl(statement)

  private def _dropForeignKeyConstraints = {

    val cs = Session.currentSession
    val dba = cs.databaseAdapter

    for(fk <- _activeForeignKeySpecs) {
      val s = cs.connection.createStatement
      dba.dropForeignKeyStatement(fk._1, dba.foreignKeyConstraintName(fk._1, fk._3.idWithinSchema), cs)
    }
  }

  private def _declareForeignKeyConstraints =
    for(fk <- _foreignKeyConstraints)
      _executeDdl(fk)

  private def _executeDdl(statement: String) = {

    val cs = Session.currentSession
    cs.log(statement)

    val s = cs.connection.createStatement
    try {
      s.execute(statement)
    }
    catch {
      case e:SQLException => throw new SquerylException("error executing " + statement + "\n" + e, e)
    }
    finally {
      s.close
    }
  }
  
  private def _foreignKeyConstraints =
    for(fk <- _activeForeignKeySpecs) yield {
      val fkDecl = fk._3

      _dbAdapter.writeForeignKeyDeclaration(
         fk._1, fkDecl.foreignKeyColumnName,
         fk._2, fkDecl.referencedPrimaryKey,
         fkDecl._referentialAction1,
         fkDecl._referentialAction2,
         fkDecl.idWithinSchema
      )
    }
  
  private def _createTables = {
    for(t <- _tables) {
      val sw = new StatementWriter(_dbAdapter)
      _dbAdapter.writeCreateTable(t, sw, this)
      _executeDdl(sw.statement)
      _dbAdapter.postCreateTable(t, None)
      for(indexDecl <- _indexDeclarationsFor(t))
        _executeDdl(indexDecl)
    }
  }

  private def _createUniqueConstraintsOfCompositePKs =
    for(cpk <- _allCompositePrimaryKeys) {
      val createConstraintStmt = _dbAdapter.writeUniquenessConstraint(cpk._1, cpk._2)
      _executeDdl(createConstraintStmt)
    }  

  /**
   * returns an Iterable of (Table[_],Iterable[FieldMetaData]), the list of
   * all tables whose PK is a composite, with the columns that are part of the PK : Iterable[FieldMetaData] 
   */
  private def _allCompositePrimaryKeys = {
    
    val res = new ArrayBuffer[(Table[_],Iterable[FieldMetaData])]
    
    for(t <- _tables
        if classOf[KeyedEntity[_]].isAssignableFrom(t.posoMetaData.clasz)) {

      Utils.mapSampleObject(
        t.asInstanceOf[Table[KeyedEntity[_]]],
        (ke:KeyedEntity[_]) => {
          val id = ke.id
          if(id.isInstanceOf[CompositeKey]) {
            val compositeCols = id.asInstanceOf[CompositeKey]._fields
            res.append((t, compositeCols))
          }
        }
      )
    }

    res
  }

  /**
   * Use this method to override the DatabaseAdapter's default column type for the given field
   * (FieldMetaData), returning None means that no override will take place.
   *
   * There are two levels at which db column type can be overriden, in order of precedence :
   *
   *   on(professors)(p => declare(
   *      s.yearlySalary is(dbType("real"))
   *    ))
   *
   *  overrides (has precedence over) :
   *
   *  MySchema extends Schema {
   *    ...
   *    override def columnTypeFor(fieldMetaData: FieldMetaData, owner: Table[_]) =
   *      if(fieldMetaData.wrappedFieldType.isInstanceOf[Int)
   *        Some("number")
   *      else
   *        None
   *  }
   *
   */
  def columnTypeFor(fieldMetaData: FieldMetaData, owner: Table[_]): Option[String] = None
  
  def tableNameFromClass(c: Class[_]):String =
    c.getSimpleName

  protected def table[T]()(implicit manifestT: Manifest[T]): Table[T] =
    table(tableNameFromClass(manifestT.erasure))(manifestT)
  
  protected def table[T](name: String)(implicit manifestT: Manifest[T]): Table[T] = {
    val typeT = manifestT.erasure.asInstanceOf[Class[T]]
    val t = new Table[T](name, typeT, this, None)
    _addTable(t)
    _addTableType(typeT, t)
    t
  }

  protected def table[T](name: String, prefix: String)(implicit manifestT: Manifest[T]): Table[T] = {
    val typeT = manifestT.erasure.asInstanceOf[Class[T]]
    val t = new Table[T](name, typeT, this, Some(prefix))
    _addTable(t)
    _addTableType(typeT, t)
    t
  }

  private [squeryl] def _addTable(t:Table[_]) =
    _tables.append(t)
    
  private [squeryl] def _addTableType(typeT: Class[_], t: Table[_]) =
    _tableTypes += ((typeT, t))
  
  protected def view[T]()(implicit manifestT: Manifest[T]): View[T] =
    view(tableNameFromClass(manifestT.erasure))(manifestT)

  protected def view[T](name: String)(implicit manifestT: Manifest[T]): View[T] =
    new View[T](name)(manifestT)

  
  class ReferentialEvent(val eventName: String) {
    def restrict = new ReferentialActionImpl("restrict", this)
    def cascade = new ReferentialActionImpl("cascade", this)
    def noAction = new ReferentialActionImpl("no action", this)
    def setNull = new ReferentialActionImpl("set null", this)
  }

  class ReferentialActionImpl(token: String, ev: ReferentialEvent) extends ReferentialAction {
    def event = ev.eventName
    def action = token
  }
  
  protected def onUpdate = new ReferentialEvent("update")

  protected def onDelete = new ReferentialEvent("delete")

  private var _fkIdGen = 1 

  private [squeryl] def _createForeignKeyDeclaration(fkColName: String, pkColName: String) = {
    val fkd = new ForeignKeyDeclaration(_fkIdGen, fkColName, pkColName)
    _fkIdGen += 1
    applyDefaultForeignKeyPolicy(fkd)
    fkd
  }

  def applyDefaultForeignKeyPolicy(foreignKeyDeclaration: ForeignKeyDeclaration) =
    foreignKeyDeclaration.constrainReference

  /**
   * @return a Tuple2 with (LengthOfDecimal, Scale) that will determine the storage
   * length of the database type that map fields of type java.lang.BigDecimal
   * Can be overridden by the Column Annotation, ex.: Column(length=22, scale=20)
   * default is (20,16)
   */
  
  def defaultSizeOfBigDecimal = (20,16)

  /**
   * @return the default database storage (column) length for String columns for this Schema,
   * Can be overridden by the Column Annotation ex.: Column(length=256)
   * default is 128 
   */
  def defaultLengthOfString = 128

  /**
   * protected since table declarations must only be done inside a Schema
   */
  protected def declare[B](a: BaseColumnAttributeAssignment*) = a

  /**
   * protected since table declarations must only be done inside a Schema
   */  
  protected def on[A](table: Table[A]) (declarations: A=>Seq[BaseColumnAttributeAssignment]) = {

    if(table == null)
      org.squeryl.internals.Utils.throwError("on function called with null argument in " + this.getClass.getName +
            " tables must be initialized before declarations.")

    val colAss: Seq[BaseColumnAttributeAssignment] =
      Utils.mapSampleObject(table, declarations)

    // all fields that have a single 'is' declaration are first reset :
    for(ca <- colAss if ca.isInstanceOf[ColumnAttributeAssignment])
      ca.clearColumnAttributes

    for(ca <- colAss) ca match {
      case dva:DefaultValueAssignment    => {

        if(! dva.value.isInstanceOf[ConstantExpressionNode[_]])
          org.squeryl.internals.Utils.throwError("error in declaration of column "+ table.prefixedName + "." + dva.left.nameOfProperty + ", " +
                "only constant expressions are supported in 'defaultsTo' declaration")

        dva.left._defaultValue = Some(dva.value.asInstanceOf[ConstantExpressionNode[_]])
      }
      case caa:ColumnAttributeAssignment => {

        for(ca <- caa.columnAttributes)
          (caa.left._addColumnAttribute(ca))

        //don't allow a KeyedEntity.id field to not have a uniqueness constraint :
        if(ca.isIdFieldOfKeyedEntityWithoutUniquenessConstraint)
          caa.left._addColumnAttribute(primaryKey)
      }
      case ctaa:ColumnGroupAttributeAssignment => {

        //don't allow a KeyedEntity.id field to not have a uniqueness constraint :
        if(ca.isIdFieldOfKeyedEntityWithoutUniquenessConstraint)
          ctaa.addAttribute(primaryKey)

        _addColumnGroupAttributeAssignment(ctaa)
      }
      
      case a:Any => org.squeryl.internals.Utils.throwError("did not match on " + a.getClass.getName)
    }

//    for(ca <- colAss.find(_.isIdFieldOfKeyedEntity))
//      assert(
//        ca.columnAttributes.exists(_.isInstanceOf[PrimaryKey]) ||
//        ca.columnAttributes.exists(_.isInstanceOf[Unique]),
//        "Column 'id' of table '" + table.name +
//        "' must have a uniqueness constraint by having the column attribute 'primaryKey' or 'unique' to honor it's KeyedEntity trait"
//      )


    // Validate that autoIncremented is not used on other fields than KeyedEntity[A].id :
    // since it is not yet unsupported :
    for(ca <- colAss) ca match {
      case cga:CompositeKeyAttributeAssignment => {}
      case caa:ColumnAttributeAssignment => {
        for(ca <- caa.columnAttributes if ca.isInstanceOf[AutoIncremented] && !(caa.left.isIdFieldOfKeyedEntity))
          org.squeryl.internals.Utils.throwError("Field " + caa.left.nameOfProperty + " of table " + table.name +
                " is declared as autoIncremented, auto increment is currently only supported on KeyedEntity[A].id")
      }
      case dva:Any => {}
    }
  }

  private def _addColumnGroupAttributeAssignment(cga: ColumnGroupAttributeAssignment) =
    _columnGroupAttributeAssignments.append(cga);
  
  def defaultColumnAttributesForKeyedEntityId(typeOfIdField: Class[_]) =
    if(typeOfIdField.isAssignableFrom(classOf[java.lang.Long]) || typeOfIdField.isAssignableFrom(classOf[java.lang.Integer]))
      Set(new PrimaryKey, new AutoIncremented(None))
    else
      Set(new PrimaryKey)
  
  protected def unique = Unique()

  protected def primaryKey = PrimaryKey()

  protected def autoIncremented = AutoIncremented(None)
  
  protected def autoIncremented(sequenceName: String) = AutoIncremented(Some(sequenceName))

  protected def indexed = Indexed(None)

  protected def indexed(indexName: String) = Indexed(Some(indexName))

  protected def dbType(declaration: String) = DBType(declaration)

  protected def uninsertable = Uninsertable()

  protected def unupdatable = Unupdatable()
  
  protected def named(name: String) = Named(name)

  class ColGroupDeclaration(cols: Seq[FieldMetaData]) {

    def are(columnAttributes: AttributeValidOnMultipleColumn*) =
      new ColumnGroupAttributeAssignment(cols, columnAttributes)
  }

  def columns(fieldList: TypedExpressionNode[_]*) = new ColGroupDeclaration(fieldList.map(_._fieldMetaData))

  // POSO Life Cycle Callbacks :

  def callbacks: Seq[LifecycleEvent] = Nil

////2.9.x approach for LyfeCycle events :
//  def delayedInit(body: => Unit) = {
//
//    body
//
//    (for(cb <- callbacks; t <- cb.target) yield (t, cb))
//    .groupBy(_._1)
//    .mapValues(_.map(_._2))
//    .foreach(
//     (t:Tuple2[View[_],Seq[LifecycleEvent]]) => {
//       t._1._callbacks = new LifecycleEventInvoker(t._2, t._1)
//     }
//    )
//  }

////2.8.x approach for LyfeCycle events :
  private [squeryl] lazy val _callbacks: Map[View[_],LifecycleEventInvoker] = {
    val m =
      (for(cb <- callbacks; t <- cb.target) yield (t, cb))
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .map(
       (t:Tuple2[View[_],Seq[LifecycleEvent]]) => {
         (t._1, new LifecycleEventInvoker(t._2, t._1)): (View[_],LifecycleEventInvoker)
       })
      .toMap
    m
  }

  import internals.PosoLifecycleEvent._

  protected def beforeInsert[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, BeforeInsert)

  protected def beforeInsert[A]()(implicit m: Manifest[A]) =
    new LifecycleEventPercursorClass[A](m.erasure, this, BeforeInsert)

  protected def beforeUpdate[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, BeforeInsert)

  protected def beforeUpdate[A]()(implicit m: Manifest[A]) =
    new LifecycleEventPercursorClass[A](m.erasure, this, BeforeUpdate)

  protected def beforeDelete[A](t: Table[A])(implicit ev : A <:< KeyedEntity[_]) =
    new LifecycleEventPercursorTable[A](t, BeforeDelete)

  protected def beforeDelete[K, A <: KeyedEntity[K]]()(implicit m: Manifest[A]) =
    new LifecycleEventPercursorClass[A](m.erasure, this, BeforeDelete)

  protected def afterInsert[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, AfterInsert)

  protected def afterInsert[A]()(implicit m: Manifest[A]) =
    new LifecycleEventPercursorClass[A](m.erasure, this, AfterInsert)

  protected def afterUpdate[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, AfterUpdate)

  protected def afterUpdate[A]()(implicit m: Manifest[A]) =
    new LifecycleEventPercursorClass[A](m.erasure, this, AfterUpdate)

  protected def afterDelete[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, AfterDelete)

  protected def afterDelete[A]()(implicit m: Manifest[A]) =
    new LifecycleEventPercursorClass[A](m.erasure, this, AfterDelete)

  protected def factoryFor[A](table: Table[A]) =
    new PosoFactoryPercursorTable[A](table)

  /**
   * Creates a ActiveRecord instance for the given the object. That allows the user
   * to save the given object using the Active Record pattern.
   *
   * @return a instance of ActiveRecord associated to the given object.
   */
  implicit def anyRef2ActiveTransaction[A](a: A)(implicit queryDsl: QueryDsl, m: Manifest[A]) =
    new ActiveRecord(a, queryDsl, m)

  /**
   * Active Record pattern implementation. Enables the user to insert an object in its
   * existent table with a convenient {{{save}}} method.
   */
  class ActiveRecord[A](a: A, queryDsl: QueryDsl, m: Manifest[A]) {
    
    private def _performAction(action: (Table[A]) => Unit) =
      _tableTypes get (m.erasure) map { table: Table[_] =>
        queryDsl inTransaction (action(table.asInstanceOf[Table[A]]))
      }
    
    /**
     * Same as {{{table.insert(a)}}}
     */
    def save =
      _performAction(_.insert(a))

    /**
     * Same as {{{table.update(a)}}}
     */  
    def update(implicit ev: A <:< KeyedEntity[_]) =
      _performAction(_.update(a))
      
  }

}
