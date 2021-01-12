import sbtassembly.AssemblyPlugin.autoImport.ShadeRule

import scala.util.Properties

name := """sequila"""
val DEFAULT_SPARK_2_VERSION = "2.4.3"
lazy val sparkVersion = Properties.envOrElse("SPARK_VERSION", DEFAULT_SPARK_2_VERSION)

version := s"${sys.env.getOrElse("VERSION", "0.1.0")}"
organization := "org.biodatageeks"
scalaVersion := "2.11.8"


val isSnapshotVersion = settingKey[Boolean]("Is snapshot")
isSnapshotVersion := version.value.toLowerCase.contains("snapshot")

val DEFAULT_HADOOP_VERSION = "2.6.5"

lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)

dependencyOverrides += "com.google.guava" % "guava" % "15.0"

libraryDependencies += "org.seqdoop" % "hadoop-bam" % "7.10.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
libraryDependencies +=  "org.apache.spark" % "spark-core_2.11" % sparkVersion
libraryDependencies +=  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-hive" % sparkVersion excludeAll (ExclusionRule("org.apache.avro"))
libraryDependencies +=  "org.apache.spark" %% "spark-hive-thriftserver" % "2.4.0" excludeAll (ExclusionRule("org.apache.avro"))
libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.11" % "2.4.0_0.11.0" % "test" excludeAll ExclusionRule(organization = "javax.servlet") excludeAll (ExclusionRule("org.apache.hadoop"))
libraryDependencies += "org.bdgenomics.adam" %% "adam-core-spark2" % "0.25.0"
libraryDependencies += "org.bdgenomics.adam" %% "adam-apis-spark2" % "0.25.0"
libraryDependencies += "org.bdgenomics.adam" %% "adam-cli-spark2" % "0.25.0"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"
libraryDependencies += "org.bdgenomics.utils" % "utils-metrics-spark2_2.11" % "0.2.16"
libraryDependencies += "com.github.samtools" % "htsjdk" % "2.19.0"
libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.13" excludeAll (ExclusionRule("org.apache.hadoop"))
libraryDependencies += "org.broadinstitute" % "gatk-native-bindings" % "1.0.0" excludeAll (ExclusionRule("org.apache.hadoop"))
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.0"
libraryDependencies += "org.hammerlab.bam" %% "load" % "1.2.0-M1"
libraryDependencies += "de.ruedigermoeller" % "fst" % "2.57"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.7"
libraryDependencies += "org.eclipse.jetty" % "jetty-servlet" % "9.3.24.v20180605"
libraryDependencies += "org.apache.derby" % "derbyclient" % "10.14.2.0"
libraryDependencies += "org.disq-bio" % "disq" % "0.3.3"
libraryDependencies += "io.projectglow" %% "glow-spark2" % "0.6.0"
libraryDependencies += "com.intel.gkl" % "gkl" % "0.8.6"

val snapshotOnly = Def.setting(
  if (isSnapshotVersion.value) {Seq("org.biodatageeks" % "bdg-performance_2.11" % "0.2-SNAPSHOT" excludeAll (ExclusionRule("org.apache.hadoop")))}
  else Seq.empty
)

libraryDependencies ++= snapshotOnly.value


avroSpecificSourceDirectories in Compile += (sourceDirectory in Compile).value / "avro/input"
avroSpecificSourceDirectories in Test += (sourceDirectory in Test).value / "avro"
sourceGenerators in Compile += (avroScalaGenerate in Compile).taskValue
watchSources ++= ((avroSpecificSourceDirectories in Compile).value ** "*.avsc").get
sourceGenerators in Test += (avroScalaGenerate in Test).taskValue
avroSpecificScalaSource in Compile := new java.io.File("src/main/org/biodatageeks/formats")
avroSpecificScalaSource in Test := new java.io.File("src/test/org/biodatageeks/formats")


fork := true
fork in Test := true
parallelExecution in Test := true

javaOptions in Test ++= Seq(
  "-Dlog4j.debug=false",
  "-Dlog4j.configuration=log4j.properties")

javaOptions ++= Seq("-Xms512M", "-Xmx8192M", "-XX:+CMSClassUnloadingEnabled" , "-Dlog4j.configuration=log4j.properties")

//fix for using with hdp warehouse connector
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
updateOptions := updateOptions.value.withLatestSnapshots(false)
outputStrategy := Some(StdoutOutput)


resolvers ++= Seq(
  "zsibio-snapshots" at "http://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/",
  "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven",
  "spring" at "http://repo.spring.io/libs-milestone/",
  "Cloudera" at "https://repository.cloudera.com/content/repositories/releases/",
  "Hortonworks" at "http://repo.hortonworks.com/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

)

//fix for hdtsdjk patch in hadoop-bam and disq
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("htsjdk.samtools.SAMRecordHelper" -> "htsjdk.samtools.SAMRecordHelperDisq").inLibrary("org.disq-bio" % "disq" % "0.3.0"),
  ShadeRule.rename("htsjdk.samtools.SAMRecordHelper" -> "htsjdk.samtools.SAMRecordHelperHadoopBAM").inLibrary("org.seqdoop" % "hadoop-bam" % "7.10.0").inProject
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("org", xs@_*) => MergeStrategy.first
  case PathList("javax", xs@_*) => MergeStrategy.first
  case PathList("com", xs@_*) => MergeStrategy.first
  case PathList("shadeio", xs@_*) => MergeStrategy.first
  case PathList("au", xs@_*) => MergeStrategy.first
  case PathList("htsjdk", xs@_*) => MergeStrategy.first
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

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishTo := {
  if (!version.value.toLowerCase.contains("snapshot"))
    sonatypePublishToBundle.value
  else {
    val nexus = "http://zsibio.ii.pw.edu.pl/nexus/repository/"
    Some("snapshots" at nexus + "maven-snapshots")
  }
}