package org.squeryl.sharding

import org.squeryl.internals.DatabaseAdapter
import com.mysql.jdbc.Connection
import org.squeryl.{SessionFactory, Session}

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


}

trait ShardingSession{

  val shardName : String
  def databaseAdapter : DatabaseAdapter
  def connectionManager : ConnectionManager

  def reader(index : Int) : Session = session(ShardingSession.ModeRead,index)
  def writer(index : Int) : Session = session(ShardingSession.ModeWrite,index)

  def session(mode : Int , index : Int) : Session = {
    val config = this.config(mode,index)
    if(config == null){
      throw new DatabaseConfigNotFoundException(shardName,ShardingSession.ModeNames(mode),index)
    }
    val session = Session.create( connectionManager.connection(shardName,mode,config),databaseAdapter)
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

object ShardingSessionFactory extends ShardingSessionFactory{

  /**
   * The name of default shard.
   * if there is only one db ( or db replication set), it's name should be this.
   */
  val DefaultShardName = "default"

}

class ShardingSessionFactory {

  var shardingSessions = Map[String,ShardingSession]()

  def addShard(shardingSession : ShardingSession)  = {
    shardingSessions +=( shardingSession.shardName -> shardingSession)
  }

  def apply(shardName : String) : ShardingSession = {
    shardingSessions(shardName)
  }

  SessionFactory.concreteFactory = Some( () =>{
    apply(ShardingSessionFactory.DefaultShardName).selectWriter
  })


}