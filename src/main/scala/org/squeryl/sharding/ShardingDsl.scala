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
   *
   */
  def use[A](shardName : String)(a : => A) : A = {
    if(hasSameShardSession(shardName,ShardingSession.ModeWrite)){
      _using(Session.currentSession,a _)
    }else{
      _using(ShardingSessionFactory(shardName).selectWriter, a _)
    }
  }

  def read[A](shardName : String)(a : => A ) : A = {
    if(hasSameShardSession(shardName,ShardingSession.ModeRead)){
      _using(Session.currentSession,a _)
    }else{
      _using(ShardingSessionFactory(shardName).selectReader, a _)
    }
  }

  def write[A](shardName : String)( a : => A) : A = {
    if(hasSameShardSession(shardName,ShardingSession.ModeWrite)){
      _using(Session.currentSession , a _)
    }else{
      val s = Session.currentSessionOption
      try {
        if(s != None) s.get.unbindFromCurrentThread
        _executeTransactionWithin(ShardingSessionFactory(shardName).selectWriter, a _)
      }
      finally {
        if(s != None) s.get.bindToCurrentThread
      }
    }
  }


  private def hasSameShardSession(shardName : String , mode : Int) : Boolean = {
    if(! Session.hasCurrentSession){
      return false
    }else{
      val session = Session.currentSession
      session.shardInfo match{
        case Some( (sn,m) ) => sn == shardName && m == mode
        case _ => false
      }
    }
  }

  private def _using[A](session: Session, a: ()=>A): A = {
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

  private def _executeTransactionWithin[A](s: Session, a: ()=>A) = {

    val c = s.connection

    if(c.getAutoCommit)
      c.setAutoCommit(false)

    var txOk = false
    try {
      val res = _using(s, a)
      txOk = true
      res
    }
    finally {
      try {
        if(txOk)
          c.commit
        else
          c.rollback
      }
      catch {
        case e:SQLException => {
          if(txOk) throw e // if an exception occured b4 the commit/rollback we don't want to obscure the original exception
        }
      }
      try{c.close}
      catch {
        case e:SQLException => {
          if(txOk) throw e // if an exception occured b4 the close we don't want to obscure the original exception
        }
      }
    }
  }
}