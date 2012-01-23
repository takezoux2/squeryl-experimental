package org.squeryl.sharding

import org.squeryl.Schema
import org.squeryl.PrimitiveTypeMode._

/**
 *
 * User: takeshita
 * Create: 12/01/22 22:44
 */

abstract class ShardedSchemaTester extends ShardedDbTestBase{

  def schema : Schema


  var executeSchemaOnReadDatabase = true

  private def shardNames = {
    if(targetShards.isEmpty) ShardingSessionFactory.allShardNames
    else targetShards
  }

  override def beforeAll(){
    super.beforeAll
    if(notIgnored){
      for(s <- shardNames){
        println("init " + s)
        use(s){
          schema.drop
          schema.create
        }
      }
    }
  }

  override def afterAll(){
    super.afterAll
    if(notIgnored){
      for(s <- shardNames){
        use(s){
          schema.drop
        }
      }
    }
  }


}