package org.squeryl.sql

import org.squeryl.Session
import org.squeryl.sharding.ShardingSessionFactory._
import org.squeryl.sharding.{ShardingSession, ShardingSessionFactory}
import com.mysql.jdbc.PreparedStatement
import java.sql.{SQLException, ResultSet}

/**
 * 
 * User: takeshita
 * Create: 11/09/22 13:22
 */

trait RawSQLSupport{

  def execute[T](shardName : String)(func : DAO => T) : T = {
    if( hasSameShardSession(shardName,ShardingSession.ModeWrite)){
      val s = Session.currentSession
      val dao = new DAO(s.connection)
      val r = func(dao)
      r
    }else{
      _executeTransactionWithin(ShardingSessionFactory(shardName).selectWriter,func)
    }
  }

  def executeWithoutTransaction[T](shardName : String)( func : DAO => T) : T = {

    if( hasSameShardSession(shardName,ShardingSession.ModeWrite)){
      val s = Session.currentSession
      val dao = new DAO(s.connection)
      val r = func(dao)
      r
    }else{
      _executeWithoutTransaction(ShardingSessionFactory(shardName).selectWriter,func)
    }
  }
  private def _executeWithoutTransaction[A](s : Session, a:DAO => A) = {
    s.use
    try{
      _using(s,a)
    }finally{
      s.unuse
      s.safeClose()
    }

  }

  private def _using[T](session : Session,func : DAO => T) = {
    val s = Session.currentSessionOption
    try {
      if(s != None) s.get.unbindFromCurrentThread
      try {
        session.bindToCurrentThread
        val dao = new DAO(session.connection)
        val r = func(dao)
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

  private def _executeTransactionWithin[A](s: Session, a: DAO => A) = {

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



  /**
   * Data Access Object
   */
  class DAO( con : java.sql.Connection){

    def exec(sql : String , params : Any*) : Int = {

      val ps = con.prepareStatement(sql)
      var index = 1
      for( p <- params){
        ps.setObject(index,p)
        index += 1
      }
      ps.executeUpdate()
    }

    def execBatch( sqls : String*) : Array[Int] = {
      val st = con.createStatement()

      sqls.foreach( sql => {
        st.addBatch( sql)
      })

      st.executeBatch()
    }

    def execQuery(sql : String, params : Any*) : ResultSet = {
      val ps = con.prepareStatement(sql)
      var index = 1
      for( p <- params){
        ps.setObject(index,p)
        index += 1
      }
      ps.executeQuery()
    }

    def execQuery[T](sql : String , rp : ResultSetProcessor[T] , prams : Any*) : List[T] = {
      var results = new scala.collection.mutable.LinkedList[T]
      val rs = execQuery(sql,prams:_*)
      rp.init(rs)
      while(rs.next){
        val o = rp.eachResult(rs)
        if(o.isDefined){
          results :+= o.get
        }
      }
      rp.done(rs)
      results.toList
    }
  }


}

trait ResultSetProcessor[T]{

  def init(resultSet : ResultSet) : Unit = {}

  def eachResult(resultSet : ResultSet) : Option[T]

  def done(resultSet : ResultSet) : Unit = {}
}