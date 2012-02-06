package org.squeryl.sharding

import org.squeryl.internals.DatabaseAdapter
import com.mysql.jdbc.Connection
import org.squeryl.{SessionFactory, Session}
import org.squeryl.logging.StatisticsListener
import java.lang.ThreadLocal
import org.squeryl.sharding.ShardingSession.SessionEntry

/**
 * Created by IntelliJ IDEA.
 * User: takezou
 * Date: 11/09/04
 * Time: 19:54
 * To change this template use File | Settings | File Templates.
 */

class DatabaseConfig(
  var url : String,
  var username : Option[String] = None,
  var password : Option[String] = None  ){

}

object ShardingSession{

  val ModeRead : Int = 0
  val ModeWrite : Int = 1

  val ModeNames : Map[Int,String] = Map[Int,String]( ModeRead -> "read",
                         ModeWrite -> "write").withDefaultValue("????")

  var shardingSessionFactory : ShardedSessionRepository = new ShardedSessionRepositoryImpl()

  class SessionEntry(var name : String, var mode : Int , var session : Session)

  val shardSessionRepo = new ThreadLocal[scala.collection.mutable.Map[String, SessionEntry]]{
    override def initialValue(): Map[String, SessionEntry] = scala.collection.mutable.HashMap.empty

    override def remove() {
      get().values.foreach(entry => {
        entry.session.close
      })
    }
  }

  def getSession(name : String, mode : Int) : Session = {
    _getSession(name) match{
      case Some(entry) => {
        if(entry.mode <= mode){
          return entry.session
        }wlse{
          val sessionFactory = shardingSessionFactory(name)
          val session = sessionFactory. 
        }
      }
      case None => {

      }
    }
  }

  private def _getSession( name : String) : Option[SessionEntry] = {
    shardSessionRepo.get().get(name)
  }

  private def _setSession( name : String , mode : Int, session : Session) : Session = {
    _getSession(name) match{
      case Some(entry) => {
        entry.session.safeClose()
      }
      case None =>
    }
    shardSessionRepo.get().update(name,new SessionEntry(name,mode,session))
  }



}

object ShardMode extends Enumeration{
  
  val Read,Write = Value
}

trait ShardingSession{

  val shardName : String
  def databaseAdapter : DatabaseAdapter
  def connectionManager : ConnectionManager

  var statisticsListener : Option[() => StatisticsListener] = None

  def reader(index : Int) : Session = session(ShardingSession.ModeRead,index)
  def writer(index : Int) : Session = session(ShardingSession.ModeWrite,index)

  def session(mode : Int , index : Int) : Session = {
    val config = this.config(mode,index)
    if(config == null){
      throw new DatabaseConfigNotFoundException(shardName,ShardingSession.ModeNames(mode),index)
    }
    val session = if(statisticsListener.isDefined){
      new Session(connectionManager.connection(shardName,mode,config),databaseAdapter,
        Some(statisticsListener.get()))
    }else{
      Session.create( connectionManager.connection(shardName,mode,config),databaseAdapter)
    }
    session.shardInfo = Some( (shardName,mode))
    session
  }

  def selectReader : Session
  def selectWriter : Session

  def getReaderConfig(index : Int ) : DatabaseConfig = config(ShardingSession.ModeRead,index)

  def getWriterConfig(index : Int) : DatabaseConfig = config(ShardingSession.ModeWrite,index)

  def config(mode : Int , index : Int) : DatabaseConfig



}

class DatabaseConfigNotFoundException(shardName : String , modeName  : String , index : Int) extends
Exception("Databse config for shard:%s mode:%s index:%s".format(shardName,modeName,index))


trait ShardedSessionRepository{

  def apply(name : String) : ShardingSession

}

class ShardedSessionRepositoryImpl extends ShardedSessionRepository {

  private var shardingSessions = Map[String,ShardingSession]()

  def allShardingSessions = shardingSessions.values
  def allShardNames = shardingSessions.keys

  def addShard(shardingSession : ShardingSession)  = {
    if(shardingSessions.size == 0){
      // register first ShardingSession as default squeryl session.
      SessionFactory.concreteFactory = Some( () =>{
        shardingSession.selectWriter
      })
    }
    shardingSessions +=( shardingSession.shardName -> shardingSession)
  }

  def apply(shardName : String, mode : ) : ShardingSession = {
    shardingSessions(shardName)
  }


}