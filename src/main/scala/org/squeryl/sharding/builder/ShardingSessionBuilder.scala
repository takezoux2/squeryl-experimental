package org.squeryl.sharding.builder

import org.squeryl.sharding.{ShardingSession, ShardingSessionFactory}

/**
 * Created by IntelliJ IDEA.
 * User: takeshita
 * Date: 11/09/07
 * Time: 23:49
 * To change this template use File | Settings | File Templates.
 */

trait ShardingSessionBuilder{
  var name : String = ShardingSessionFactory.DefaultShardName

  def create() : ShardingSession = create(this.name)
  def create( _name : String) : ShardingSession
}