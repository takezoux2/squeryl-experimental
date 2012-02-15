package org.squeryl.sql

import org.squeryl.Session
import com.mysql.jdbc.PreparedStatement
import java.sql.{SQLException, ResultSet}
import org.squeryl.sharding._

/**
 * 
 * User: takeshita
 * Create: 11/09/22 13:22
 */

trait RawSQLSupport{

  def shardedSessionCache : ShardedSessionCache

  def execute[T](shardName : String)(func : DAO => T) : T = {
    val session = shardedSessionCache.getSession(shardName,ShardMode.Write)
    
    session.use()
    session.beginTransaction()
    var txOk = false
    
    try{
      val connection = session.connection
      val dao = new DAO(connection)
      val r = func(dao)
      txOk = true
      r
    }finally{
      if(txOk){
        session.commitTransaction()
      }else{
        session.rollback()
      }
      if(session.safeClose()){
        shardedSessionCache.removeSession(session)
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

    def execQuery[T](sql : String, proc : ResultSet => T , params : Any*) : List[T] = {
      var results = new scala.collection.mutable.LinkedList[T]

      val rs = execQuery(sql,params:_*)
      while(rs.next){
        results :+= proc(rs)
      }
      results.toList
    }
  }


}

trait ResultSetProcessor[T]{

  def init(resultSet : ResultSet) : Unit = {}

  def eachResult(resultSet : ResultSet) : Option[T]

  def done(resultSet : ResultSet) : Unit = {}
}