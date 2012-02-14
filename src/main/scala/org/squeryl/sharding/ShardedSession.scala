package org.squeryl.sharding

import org.squeryl.Session

/**
 * Created by IntelliJ IDEA.
 * User: takezoux3
 * Date: 12/02/06
 * Time: 23:56
 * To change this template use File | Settings | File Templates.
 */


object ShardedSession{



  var shardedSessionRepository : ShardedSessionRepository = new ShardedSessionRepositoryImpl()


  val shardedSessions = new ThreadLocal[scala.collection.mutable.Map[String, ShardedSession]]{
    override def initialValue() = scala.collection.mutable.HashMap.empty

    override def remove() {
      get().values.foreach(entry => {
        entry.session.close
      })
    }
  }

  def getSession(name : String, mode : ShardMode.Value) : ShardedSession = {
    _getSession(name) match{
      case Some(entry) => {
        if(ShardMode.hasSameFunction(mode,entry.shardMode)){
          entry
        }else{
          entry.forceClose()
          _createSession(name,mode)
        }
      }
      case None => {
        _createSession(name,mode)
      }
    }
  }
  def removeSession(session : ShardedSession) = {
    shardedSessions.get().remove(session.shardName)
  }
  
  private def _createSession(name : String , mode : ShardMode.Value) = {
    val session = shardedSessionRepository(name,mode)
    shardedSessions.get() +=(name -> session)
    session
  }

  private def _getSession( name : String) : Option[ShardedSession] = {
    shardedSessions.get().get(name)
  }

  private def _setSession( name : String , mode : ShardMode.Value, session : Session) : ShardedSession = {
    val shardedSession = new ShardedSession(name,mode,session)
    shardedSessions.get().update(name,shardedSession)
    shardedSession
  }



}

case class ShardedSession(shardName : String , shardMode : ShardMode.Value,session : Session){

  private var useCounter = 1
  def safeClose() : Boolean = {
    useCounter -= 1
    if(useCounter <= 0){
      session.close
      true
    }else{
      false
    }
  }

  def use() {
    useCounter += 1
  }

  def forceClose()  : Boolean = {
    if(useCounter > 0){
      session.close
      useCounter = 0
      true
    }else{
      false
    }
  }
  
  private var transactionCounter = 0
  
  def beginTransaction() = {
    if(transactionCounter == 0){
      val c = session.connection
      if(c.getAutoCommit()){
        c.setAutoCommit(false)
      }
    }
  }
  
  def commitTransaction() = {
    transactionCounter -= 1
    if(transactionCounter <= 0){
      val c = session.connection
      c.commit
    }
  }
  
  def rollback() = {
    if(transactionCounter > 0){
      transactionCounter = 0
      session.connection.rollback()
    }
  }

}

object ShardMode extends Enumeration{

  val Read = Value(0,"read")
  val Write = Value(1,"write")

  def hasSameFunction( val1 : ShardMode.Value , val2: ShardMode.Value) = {
    val1.id <= val2.id
  }
}
