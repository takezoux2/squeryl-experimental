package org.squeryl.sharding

import org.squeryl.internals.DatabaseAdapter
import org.squeryl.SquerylException
import util.Random

/**
 * Simple sharding session
 * User: takezou
 * Date: 11/09/04
 * Time: 21:15
 * To change this template use File | Settings | File Templates.
 */

class SimpleShardingSession(val shardName : String,
                            val connectionManager : ConnectionManager,
                            val databaseAdapter : DatabaseAdapter) extends ShardingSession {

  protected var readerConfigs : List[DatabaseConfig] = Nil
  protected var writerConfigs : List[DatabaseConfig] = Nil
  val random = new Random
  def selectWriter = {
    writer(random.nextInt(writerConfigs.size))
  }

  def selectReader = {
    reader(random.nextInt(readerConfigs.size))
  }

  def config(mode: Int, index: Int) = {
    if(mode == ShardingSession.ModeRead){
      readerConfigs(index)
    }else if(mode == ShardingSession.ModeWrite){
      writerConfigs(index)
    }else{
      throw new SquerylException("Not supported mode")
    }
  }

  def addConfig( mode : Int , config : DatabaseConfig) = {
    if(mode == ShardingSession.ModeRead){
      readerConfigs = readerConfigs :+ config
    }else if(mode == ShardingSession.ModeWrite){
      writerConfigs = writerConfigs :+ config
    }else{
      throw new SquerylException("Not supported mode")
    }
  }

}