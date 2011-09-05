
name := "squeryl"

organization := "org.squeryl"

version := "0.9.5-Shard-SNAPSHOT"


scalaVersion := "2.9.0"


libraryDependencies += "cglib" % "cglib-nodep" % "2.2"

libraryDependencies += "org.scala-lang" % "scalap" % "2.9.0"

  
libraryDependencies += "com.h2database" % "h2" % "1.2.127" % "provided"
  
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.10" % "provided"
	
libraryDependencies += "postgresql" % "postgresql" % "8.4-701.jdbc4" % "provided"
  
libraryDependencies += "net.sourceforge.jtds" % "jtds" % "1.2.4" % "provided"

libraryDependencies += "org.apache.derby" % "derby" % "10.7.1.1" % "provided"

    
libraryDependencies += "junit" % "junit" % "4.8.2" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.4.1" % "provided" 
