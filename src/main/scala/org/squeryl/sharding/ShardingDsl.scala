package org.squeryl.sharding

import java.sql.{SQLException, ResultSet}
import org.squeryl.{SessionFactory, Session}

/**
 * Created by IntelliJ IDEA.
 * User: takezou
 * Date: 11/09/04
 * Time: 23:03
 * To change this template use File | Settings | File Templates.
 */

trait ShardingDsl {

  val shardedSessionProxy = new ShardedSessionProxy
  /**
   * exec in write mode without transaction
   */
  def use[A](shardName : String)(a : => A) : A = {
    val session = shardedSessionProxy.getSession(shardName,ShardMode.Write)
    _executeWithoutTransaction(session,a _)
  }

  /**
   * exec in read mode
   */
  def read[A](shardName : String)(a : => A ) : A = {
    val session = shardedSessionProxy.getSession(shardName,ShardMode.Read)
    _executeWithoutTransaction(session,a _)
  }

  /**
   * exec in write mode with transaction
   */
  def write[A](shardName : String)( a : => A) : A = {
    val session = shardedSessionProxy.getSession(shardName,ShardMode.Write)
    _executeTransactionWithin(session,a _)
  }



  private def _executeWithoutTransaction[A](s : ShardedSession, a:() => A) : A = {
    s.use()
    try{
      _using(s,a)
    }finally{
      if(s.safeClose()){
        shardedSessionProxy.removeSession(s)
      }
    }

  }


  private def _executeTransactionWithin[A](s: ShardedSession, a: ()=>A) = {

    s.use()
    s.beginTransaction()
    var txOk = false
    try{
      val res = _using(s,a)
      s.commitTransaction()
      if(s.safeClose){
        shardedSessionProxy.removeSession(s)
      }
      txOk = true
      res
    }catch{
      case e : Exception => {
        _ignoreException(s.rollback())
        _ignoreException(s.forceClose())
        _ignoreException(shardedSessionProxy.removeSession(s))
        throw e
      }
    }
  }

  @inline
  private def _ignoreException(func : => Unit) = {
    try{
      func
    }catch{
      case e : Exception => e.printStackTrace()
    }
    
  }

  private def _using[A](session: ShardedSession, a: ()=>A): A = {
    val s = Session.currentSessionOption
    try {
      if(s != None) s.get.unbindFromCurrentThread
      try {
        session.bindToCurrentThread
        val r = a()
        r
      }
      finally {
        session.unbindFromCurrentThread
        session.cleanup
      }
    }
    finally {
      if(s != None) s.get.bindToCurrentThread
    }
  }
}