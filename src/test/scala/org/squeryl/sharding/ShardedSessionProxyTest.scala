package org.squeryl.sharding

import org.scalatest.FlatSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.{JMockCycle, EasyMockSugar}
import org.jmock.Expectations._
import org.junit.runner.RunWith
import org.scalatest.junit.{JUnitRunner, JUnitSuite}

/**
 *
 * User: takeshita
 * Create: 12/02/15 1:45
 */

@RunWith(classOf[JUnitRunner])
class ShardedSessionProxyTest extends FlatSpec with MustMatchers{

  val cycle = new JMockCycle
  import cycle._


  val mockRepo = mock[ShardedSessionRepository]
  val session = mock[ShardedSession]

  def dSession(name : String,mode : ShardMode.Value) : ShardedSession = {
    new DummyShardedSession(name,mode)
  }


  class ShardedSessionProxyTestImpl extends ShardedSessionProxy{
    shardedSessionRepository = mockRepo
  }

  "Same mode getSession calls" should "create session once" in{
    val shardedSessionProxy = new ShardedSessionProxyTestImpl
    val shardName1 = "shard1"
    val shardName2 = "shard2"
    expecting{  e => import e._
      oneOf(mockRepo).apply(shardName1,ShardMode.Read);will(returnValue(dSession(shardName1,ShardMode.Read)))
      oneOf(mockRepo).apply(shardName2,ShardMode.Write);will(returnValue(dSession(shardName2,ShardMode.Write)))
    }
    whenExecuting {
      shardedSessionProxy.getSession(shardName1,ShardMode.Read) must not be(null)
      shardedSessionProxy.getSession(shardName2,ShardMode.Write) must not be(null)
      shardedSessionProxy.getSession(shardName2,ShardMode.Write) must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Read) must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Read) must not be(null)
      shardedSessionProxy.getSession(shardName2,ShardMode.Write) must not be(null)
      shardedSessionProxy.getSession(shardName2,ShardMode.Write) must not be(null)
    }
  }

  "getSession Mode Read -> Wirte calls" should "create session twice" in{
    val shardedSessionProxy = new ShardedSessionProxyTestImpl
    val shardName1 = "shard1"

    expecting{  e => import e._
      oneOf(mockRepo).apply(shardName1,ShardMode.Read);will(returnValue(session))
      allowing(session).shardMode;will(returnValue(ShardMode.Read))
      oneOf(session).forceClose() // read session is closed
      oneOf(mockRepo).apply(shardName1,ShardMode.Write);will(returnValue(dSession(shardName1,ShardMode.Write)))
      //oneOf(session).forceClose()
    }
    whenExecuting {
      shardedSessionProxy.getSession(shardName1,ShardMode.Read) must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Read) must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Write) must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Write) must not be(null)
    }
  }

  "getSession Mode Write -> Read calls" should "create session once" in{
    val shardedSessionProxy = new ShardedSessionProxyTestImpl
    val shardName1 = "shard1"

    expecting{  e => import e._
      oneOf(mockRepo).apply(shardName1,ShardMode.Write);will(returnValue(dSession(shardName1,ShardMode.Write)))
    }
    whenExecuting {
      shardedSessionProxy.getSession(shardName1,ShardMode.Write) must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Write) must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Read) must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Read) must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Write) must not be(null)
    }
  }

  "getSession calls" should "create session after remove session" in{
    val shardedSessionProxy = new ShardedSessionProxyTestImpl
    val shardName1 = "shard1"

    expecting{  e => import e._
      oneOf(mockRepo).apply(shardName1,ShardMode.Write);will(returnValue(dSession(shardName1,ShardMode.Write)))
      oneOf(mockRepo).apply(shardName1,ShardMode.Write);will(returnValue(dSession(shardName1,ShardMode.Write)))
    }
    whenExecuting {
      val ses =  shardedSessionProxy.getSession(shardName1,ShardMode.Write)
      ses must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Write) must not be(null)
      shardedSessionProxy.removeSession(ses)
      shardedSessionProxy.getSession(shardName1,ShardMode.Write) must not be(null)
      shardedSessionProxy.getSession(shardName1,ShardMode.Write) must not be(null)
    }
  }



}