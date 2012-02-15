package org.squeryl.sharding

import builder.SimpleShardedSessionBuilder
import org.scalatest._
import matchers.{MustMatchers, ShouldMatchers}
import org.squeryl.{PrimitiveTypeMode, SessionFactory}

/**
 *
 * User: takeshita
 * Create: 12/01/22 22:32
 */

abstract class ShardedDbTestBase extends FunSuite with MustMatchers with BeforeAndAfterAll with BeforeAndAfterEach {

  def targetShards : List[String]

  def initializeSessions() : Boolean

  var notIgnored = true

  val ignoredTests : List[String] = Nil

  override def beforeAll(){
    super.beforeAll
    notIgnored = initializeSessions()
  }

  override protected def runTest(testName: String,
                                 reporter: Reporter,
                                 stopper: Stopper,
                                 configMap: Map[String, Any],
                                 tracker: Tracker) {

    if(!notIgnored || ignoredTests.find(_ == testName).isDefined){
      //reporter(TestIgnored(new Ordinal(0), suiteName, Some(this.getClass.getName),testName))
      return
    }
    super.runTest(testName, reporter, stopper, configMap, tracker)
  }
}

trait SimpleShardingBuilderInitializer{

  var _targetShards : List[String] = Nil
  def targetShards = _targetShards

  def initializeSessions() : Boolean = {

    for(settingSet <- shardSettings){
      val builder = createBuilder()
      
      builder.name = settingSet._1
      _targetShards = _targetShards :+ builder.name
      for(c <- settingSet._2){
        builder.addWriter(c)
      }
      for(c <- settingSet._3){
        builder.addReader(c)
      }
      val repos = new ShardedSessionRepositoryImpl()
      repos.addFactory(builder.create())
      PrimitiveTypeMode.shardedSessionCache.shardedSessionRepository = repos


    }

    true
  }

  def createBuilder() = {
    new SimpleShardedSessionBuilder()
  }

  /**
   *
   * List of
   *  (ShardName , WriterSettings , ReaderSettings
   *
   *
   */
  def shardSettings : List[(String, List[DatabaseConfig],List[DatabaseConfig])]

}
