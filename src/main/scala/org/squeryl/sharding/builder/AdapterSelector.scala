package org.squeryl.sharding.builder

import org.squeryl.internals.DatabaseAdapter
import org.squeryl.SquerylException
import org.squeryl.adapters._

/**
 * Select appropriate adapter from url
 * User: takeshita
 * Date: 11/09/07
 * Time: 23:56
 * To change this template use File | Settings | File Templates.
 */

object AdapterSelector extends AdapterSelector

class AdapterSelector{

  /**
   *
   * @return Option[(driver.class.name , DatabaseAdapter)]
   */
  def apply(url : String) : Option[DatabaseAdapter] = {
    val splits = url.split(":")
    if(splits.length < 2){
      throw new SquerylException("Bad url")
    }
    if(splits(0) != "jdbc"){
      throw new SquerylException("Not jdbc url")
    }

    splits(1).toLowerCase match{
      case "mysql" => Some(new MySQLInnoDBAdapter)
      case "h2" => Some(new H2Adapter())
      case "postgresql" => Some(new PostgreSqlAdapter())
      case "oracle" => Some(new OracleAdapter())
      case "db2" => Some(new DB2Adapter())
      case "derby" => Some(new DerbyAdapter())
      case _ => None
    }

  }

  def getDriverClassName(databaseAdapter : DatabaseAdapter) : String = {
    databaseAdapter match{
      case a : MySQLInnoDBAdapter => "com.mysql.jdbc.Driver"
      case a : MySQLAdapter => "com.mysql.jdbc.Driver"
      case a : H2Adapter => "org.h2.Driver"
      case a : PostgreSqlAdapter => "org.postgresql.Driver"
      case a : OracleAdapter => "oracle.jdbc.driver.OracleDriver"
      case a : DB2Adapter => "com.ibm.db2.jcc.DB2Driver"
      case a : DerbyAdapter => "org.apache.derby.jdbc.EmbeddedDriver"
      case _ => null
    }
  }

}