package org.squeryl.sharding.builder

import org.squeryl.internals.DatabaseAdapter
import org.squeryl.adapters.MySQLInnoDBAdapter
import org.squeryl.SquerylException
import org.squeryl.sharding._
import org.squeryl.logging.StatisticsListener

/**
 * 簡易のセットアップ手順を提供するBuilderクラス
 * User: takeshita
 * Date: 11/09/07
 * Time: 23:38
 * To change this template use File | Settings | File Templates.
 */

class SimpleShardingSessionBuilder extends ShardingSessionBuilder{

  var adapter : DatabaseAdapter = new MySQLInnoDBAdapter

  var connectionManager : ConnectionManager = new AlwaysCreateConnectionManager()

  protected var readerConfigs : List[DatabaseConfig] = Nil
  protected var writerConfigs : List[DatabaseConfig] = Nil

  /**
   * Set appropriate adapter detected from JDBC url
   */
  var autoDetectAdapter = true
  /**
   * Load Driver class automatically
   */
  var autoLoadDriverClass = true

  /**
   * Use for detecting adapter and driver class
   */
  var adapterSelector = AdapterSelector


  var enableConsoleStatisticsListener = false

  var statisticsListener : Option[() => StatisticsListener] = None


  def addReader(config : DatabaseConfig) : SimpleShardingSessionBuilder = {
    readerConfigs = readerConfigs :+ config
    this
  }

  def addReader(url : String , username : String , password : String) : SimpleShardingSessionBuilder = {
    readerConfigs = readerConfigs :+ new DatabaseConfig(url,Some(username),Some(password))
    this
  }
  def addReader(url : String) : SimpleShardingSessionBuilder = {
    readerConfigs = readerConfigs :+ new DatabaseConfig(url)
    this
  }
  def addWriter(config : DatabaseConfig) : SimpleShardingSessionBuilder = {
    writerConfigs = writerConfigs :+ config
    this
  }
  def addWriter(url : String , username : String , password : String) : SimpleShardingSessionBuilder = {
    writerConfigs = writerConfigs :+ new DatabaseConfig(url,Some(username),Some(password))
    this
  }
  def addWriter(url : String) : SimpleShardingSessionBuilder = {
    writerConfigs = writerConfigs :+ new DatabaseConfig(url)
    this
  }

  def addBoth(config : DatabaseConfig) = {
    addReader(config)
    addWriter(config)
    this
  }

  def create(_name: String) = {
    if(writerConfigs.isEmpty){
      throw new NoDatabaseConfigException()
    }

    val adapter = if(autoDetectAdapter){
      adapterSelector(writerConfigs(0).url) match{
        case Some(adap) => adap
        case None => throw new SquerylException("Can't detect appropriate adapter for url %s".format(
          writerConfigs(0).url
        ))
      }
    }else this.adapter

    val session = new SimpleShardingSession(
      _name,
      connectionManager,
      adapter
    )
    writerConfigs.foreach(c => {
      session.addConfig(ShardingSession.ModeWrite,c)
    })
    // if there is no reader configs,use writer configs instead.
    if(readerConfigs.isEmpty){
      writerConfigs.foreach(c => {
        session.addConfig(ShardingSession.ModeRead,c)
      })
    }else{
      readerConfigs.foreach(c => {
        session.addConfig(ShardingSession.ModeRead,c)
      })
    }

    if(autoLoadDriverClass){
      val driverClassName = adapterSelector.getDriverClassName(adapter)
      if(driverClassName == null){
        throw new SquerylException("Can't detect driver class. Adapter:" + adapter.getClass.getName)
      }
      Class.forName(driverClassName)
    }

    if(statisticsListener.isDefined){
      session.statisticsListener = statisticsListener
    }else if(enableConsoleStatisticsListener){
      session.statisticsListener = Some( () => ConsoleStatisticsListener)
    }

    session
  }
}

class NoDatabaseConfigException extends SquerylException("There is no database config." +
  "At lease one writer config is needed"){

}
