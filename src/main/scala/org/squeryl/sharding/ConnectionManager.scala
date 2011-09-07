package org.squeryl.sharding

import java.sql.Connection
import java.sql.DriverManager

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

class AlwaysCreateConnectionManager extends ConnectionManager{
  def connection(shardName: String, mode: Int, config: DatabaseConfig) = {
    if(config.username.isDefined && config.password.isDefined){
      DriverManager.getConnection(config.url,config.username.get,config.password.get)
    }else{
      DriverManager.getConnection(config.url)
    }
  }
}