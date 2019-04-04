import scala.util.Properties

name := """bdg-sequila"""

version := "0.5.3-spark-2.4.0-SNAPSHOT"

organization := "org.biodatageeks"

scalaVersion := "2.11.8"

val DEFAULT_SPARK_2_VERSION = "2.4.0"
val DEFAULT_HADOOP_VERSION = "2.6.5"


lazy val sparkVersion = Properties.envOrElse("SPARK_VERSION", DEFAULT_SPARK_2_VERSION)
lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)




dependencyOverrides += "com.google.guava" % "guava" % "15.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
libraryDependencies +=  "org.apache.spark" % "spark-core_2.11" % sparkVersion

libraryDependencies +=  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-hive-thriftserver" % "2.4.0"

libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.11" % "2.4.0_0.11.0" % "test" excludeAll ExclusionRule(organization = "javax.servlet") excludeAll (ExclusionRule("org.apache.hadoop"))

//libraryDependencies += "org.apache.spark" %% "spark-hive"       % "2.0.0" % "test"

libraryDependencies += "org.bdgenomics.adam" %% "adam-core-spark2" % "0.25.0"
libraryDependencies += "org.bdgenomics.adam" %% "adam-apis-spark2" % "0.25.0"
libraryDependencies += "org.bdgenomics.adam" %% "adam-cli-spark2" % "0.25.0"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"


libraryDependencies += "org.hammerlab.bdg-utils" %% "cli" % "0.3.0"

libraryDependencies += "com.github.samtools" % "htsjdk" % "2.18.2"


libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.13" excludeAll (ExclusionRule("org.apache.hadoop"))

libraryDependencies += "org.broadinstitute" % "gatk-native-bindings" % "1.0.0" excludeAll (ExclusionRule("org.apache.hadoop"))
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.0"

libraryDependencies += "com.intel.gkl" % "gkl" % "0.8.5-1-darwin-SNAPSHOT"
libraryDependencies += "com.intel.gkl" % "gkl" % "0.8.5-1-linux-SNAPSHOT"

libraryDependencies += "org.hammerlab.bam" %% "load" % "1.2.0-M1"

libraryDependencies += "de.ruedigermoeller" % "fst" % "2.57"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.7"
libraryDependencies += "org.eclipse.jetty" % "jetty-servlet" % "9.3.24.v20180605"
libraryDependencies += "org.apache.derby" % "derbyclient" % "10.14.2.0"


libraryDependencies += "org.biodatageeks" % "bdg-performance_2.11" % "0.2-SNAPSHOT" excludeAll (ExclusionRule("org.apache.hadoop"))




fork := true
fork in Test := true
//parallelExecution in Test := false
//javaOptions in Test += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=9999"
//javaOptions in run += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=9999"

//javaOptions in run += "-Dseq.metastore.dir=/Users/marek/data/output/metastore"

javaOptions in Test ++= Seq(
  "-Dlog4j.debug=false",
  "-Dlog4j.configuration=log4j.properties")

javaOptions ++= Seq("-Xms512M", "-Xmx8192M", "-XX:+CMSClassUnloadingEnabled")

//fix for using with hdp warehouse connector
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

updateOptions := updateOptions.value.withLatestSnapshots(false)

outputStrategy := Some(StdoutOutput)


resolvers ++= Seq(
  "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven",
  "zsibio-snapshots" at "http://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/",
  "spring" at "http://repo.spring.io/libs-milestone/",
  "Cloudera" at "https://repository.cloudera.com/content/repositories/releases/",
  "Hortonworks" at "http://repo.hortonworks.com/content/repositories/releases/"
)


assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("org", xs@_*) => MergeStrategy.first
  case PathList("javax", xs@_*) => MergeStrategy.first
  case PathList("com", xs@_*) => MergeStrategy.first
  case PathList("shadeio", xs@_*) => MergeStrategy.first

  case PathList("au", xs@_*) => MergeStrategy.first
  case ("META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat") => MergeStrategy.first
  case ("images/ant_logo_large.gif") => MergeStrategy.first

  case "overview.html" => MergeStrategy.rename
  case "mapred-default.xml" => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "parquet.thrift" => MergeStrategy.last
  case "plugin.xml" => MergeStrategy.last
  case "codegen/config.fmpp" => MergeStrategy.last
  case "git.properties" => MergeStrategy.last

  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

/* only for releasing assemblies*/
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)

publishConfiguration := publishConfiguration.value.withOverwrite(true)

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
publishTo := {
  val nexus = "http://zsibio.ii.pw.edu.pl/nexus/repository/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "maven-snapshots")
  else
    Some("releases" at nexus + "maven-releases")
}
