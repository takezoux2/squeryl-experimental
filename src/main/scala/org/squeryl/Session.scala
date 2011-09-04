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

import logging.StatisticsListener
import org.squeryl.internals._
import collection.mutable.ArrayBuffer
import java.sql.{SQLException, ResultSet, Statement, Connection}


class Session(val connection: Connection, val databaseAdapter: DatabaseAdapter, val statisticsListener: Option[StatisticsListener] = None) {

  var shardInfo : Option[(String,Int)] = None

  def bindToCurrentThread = Session.currentSession = Some(this)

  def unbindFromCurrentThread = Session.currentSession = None

  private var _logger: String => Unit = null

  def logger_=(f: String => Unit) = _logger = f

  def setLogger(f: String => Unit) = _logger = f

  def isLoggingEnabled = _logger != null

  def log(s:String) = if(isLoggingEnabled) _logger(s)

  var logUnclosedStatements = false

  private val _statements = new ArrayBuffer[Statement]

  private val _resultSets = new ArrayBuffer[ResultSet]

  private [squeryl] def _addStatement(s: Statement) = _statements.append(s)

  private [squeryl] def _addResultSet(rs: ResultSet) = _resultSets.append(rs)

  def cleanup = {
    _statements.foreach(s => {
      if (logUnclosedStatements && isLoggingEnabled && !s.isClosed) {
        val stackTrace = Thread.currentThread.getStackTrace.map("at " + _).mkString("\n")
        log("Statement is not closed: " + s + ": " + System.identityHashCode(s) + "\n" + stackTrace)
      }
      Utils.close(s)
    })
    _statements.clear
    _resultSets.foreach(rs => Utils.close(rs))
    _resultSets.clear
  }

  def close = {
    cleanup
    connection.close
  }
}

trait SessionFactory {
  def newSession: Session
}

object SessionFactory {

  /**
   * Initializing concreteFactory with a Session creating closure enables the use of
   * the 'transaction' and 'inTransaction' block functions 
   */
  var concreteFactory: Option[()=>Session] = None

  /**
   * Initializing externalTransactionManagementAdapter with a Session creating closure allows to
   * execute Squeryl statements *without* the need of using 'transaction' and 'inTransaction'.
   * The use case for this is to allow Squeryl connection/transactions to be managed by an
   * external framework. In this case Session.cleanupResources *needs* to be called when connections
   * are closed, otherwise statement of resultset leaks can occur. 
   */
  var externalTransactionManagementAdapter: Option[()=>Session] = None

  def newSession: Session =
      concreteFactory.getOrElse(
        org.squeryl.internals.Utils.throwError("org.squeryl.SessionFactory not initialized, SessionFactory.concreteFactory must be assigned a \n"+
              "function for creating new org.squeryl.Session, before transaction can be used.\n" +
              "Alternatively SessionFactory.externalTransactionManagementAdapter can initialized, please refer to the documentation.")
      ).apply        
}

object Session {

  private val _currentSessionThreadLocal = new ThreadLocal[Option[Session]] {
    override def initialValue = None
  }
  
  def create(c: Connection, a: DatabaseAdapter) =
    new Session(c,a)  

  def currentSessionOption: Option[Session] =
    _currentSessionThreadLocal.get

  def currentSession: Session =
    if(SessionFactory.externalTransactionManagementAdapter != None) {
      val s = SessionFactory.externalTransactionManagementAdapter.get.apply
      s.bindToCurrentThread
      s
    }
    else currentSessionOption.getOrElse(
      org.squeryl.internals.Utils.throwError("no session is bound to current thread, a session must be created via Session.create \nand bound to the thread via 'work' or 'bindToCurrentThread'"))

  def hasCurrentSession =
    _currentSessionThreadLocal.get != None

  def cleanupResources =
    if(_currentSessionThreadLocal.get != None)
      _currentSessionThreadLocal.get.get.cleanup

  private def currentSession_=(s: Option[Session]) = _currentSessionThreadLocal.set(s)
}
