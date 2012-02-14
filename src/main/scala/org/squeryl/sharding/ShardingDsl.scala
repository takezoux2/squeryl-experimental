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

  /**
   * exec in write mode without transaction
   */
  def use[A](shardName : String)(a : => A) : A = {
    val session = ShardedSession.getSession(shardName,ShardMode.Write)
    _executeWithoutTransaction(session,a _)
  }

  /**
   * exec in read mode
   */
  def read[A](shardName : String)(a : => A ) : A = {
    val session = ShardedSession.getSession(shardName,ShardMode.Read)
    _executeWithoutTransaction(session,a _)
  }

  /**
   * exec in write mode with transaction
   */
  def write[A](shardName : String)( a : => A) : A = {
    val session = ShardedSession.getSession(shardName,ShardMode.Write)
    _executeTransactionWithin(session,a _)
  }



  private def _executeWithoutTransaction[A](s : ShardedSession, a:() => A) : A = {
    s.use()
    try{
      _using(s,a)
    }finally{
      if(s.safeClose()){
        ShardedSession.removeSession(s)
      }
    }

  }

  private def _using[A](session: ShardedSession, a: ()=>A): A = {
    val s = Session.currentSessionOption
    try {
      if(s != None) s.get.unbindFromCurrentThread
      try {
        session.session.bindToCurrentThread
        val r = a()
        r
      }
      finally {
        session.session.unbindFromCurrentThread
        session.session.cleanup
      }
    }
    finally {
      if(s != None) s.get.bindToCurrentThread
    }
  }

  private def _executeTransactionWithin[A](s: ShardedSession, a: ()=>A) = {

    s.beginTransaction()
    var txOk = false
    s.use()
    try{
      val res = _using(s,a)
      txOk = true
      res
    }finally {
      try {
        if(txOk)
          s.commitTransaction()
        else
          s.rollback()

      }
      catch {
        case e:SQLException => {
          if(txOk) throw e // if an exception occured b4 the commit/rollback we don't want to obscure the original exception
        }
      }
      try{
        if(s.safeClose){
          ShardedSession.removeSession(s)
        }
      }
      catch {
        case e:SQLException => {
          if(txOk) throw e // if an exception occured b4 the close we don't want to obscure the original exception
        }
      }
    }

  }
}