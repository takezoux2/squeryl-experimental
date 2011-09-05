package org.squeryl.sharding

import java.sql.Connection

/**
 * Created by IntelliJ IDEA.
 * User: takezou
 * Date: 11/09/04
 * Time: 21:16
 * To change this template use File | Settings | File Templates.
 */

trait ConnectionManager {

  def connection(shardName : String , mode : Int , config : DatabaseConfig) : Connection

}