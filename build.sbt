import sbtassembly.AssemblyPlugin.autoImport.ShadeRule

import scala.util.Properties

name := """sequila"""
val DEFAULT_SPARK_3_VERSION = "3.1.2"
lazy val sparkVersion = Properties.envOrElse("SPARK_VERSION", DEFAULT_SPARK_3_VERSION)

version := s"${sys.env.getOrElse("VERSION", "0.1.0")}"
organization := "org.biodatageeks"
scalaVersion := "2.12.13"


val isSnapshotVersion = settingKey[Boolean]("Is snapshot")
isSnapshotVersion := version.value.toLowerCase.contains("snapshot")

val DEFAULT_HADOOP_VERSION = "2.7.4"

lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)

dependencyOverrides += "com.google.guava" % "guava" % "15.0"

//removing hadoop-bam to used a patched one with support for htsjdk 2.22
//libraryDependencies += "org.seqdoop" % "hadoop-bam" % "7.10.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3"
libraryDependencies += "com.github.mrpowers" %% "spark-daria" % "0.38.2"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.1.2_1.1.0" % "test" excludeAll ExclusionRule(organization = "javax.servlet") excludeAll (ExclusionRule("org.apache.hadoop"))
libraryDependencies += "org.bdgenomics.adam" %% "adam-core-spark3" % "0.36.0" excludeAll (ExclusionRule("org.seqdoop"))
libraryDependencies += "org.bdgenomics.adam" %% "adam-apis-spark3" % "0.36.0" excludeAll (ExclusionRule("org.seqdoop"))
libraryDependencies += "org.bdgenomics.adam" %% "adam-cli-spark3" % "0.36.0" excludeAll (ExclusionRule("org.seqdoop"))
libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"
libraryDependencies += "com.github.samtools" % "htsjdk" % "2.24.1"
libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.17" excludeAll (ExclusionRule("org.apache.hadoop"))
libraryDependencies += "org.broadinstitute" % "gatk-native-bindings" % "1.0.0" excludeAll (ExclusionRule("org.apache.hadoop"))
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.0"
libraryDependencies += "de.ruedigermoeller" % "fst" % "2.57"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.7"
libraryDependencies += "org.eclipse.jetty" % "jetty-servlet" % "9.3.24.v20180605"
libraryDependencies += "org.apache.derby" % "derbyclient" % "10.14.2.0"
//libraryDependencies += "org.disq-bio" % "disq" % "0.3.8" <-disabled since we use patched version of HtsjdkReadsTraversalParameters
libraryDependencies += "io.projectglow" %% "glow-spark3" % "1.0.1" excludeAll (ExclusionRule("com.github.samtools")) excludeAll (ExclusionRule("org.seqdoop")) //FIXME:: remove togehter with disq
libraryDependencies += "com.intel.gkl" % "gkl" % "0.8.6"
libraryDependencies += "org.openjdk.jol" % "jol-core" % "0.16" % "provided"
libraryDependencies += "com.github.jsr203hadoop" % "jsr203hadoop" % "1.0.3"





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

//FIXME: limit inlining to only package scope !!!!
scalacOptions ++= Seq(
  "-opt:-l:none,_",
  "-opt-inline-from:org.biodatageeks.**",
  "-Yopt-log-inline", "_",
)

scalacOptions in assembly ++= Seq(
  "-opt:-l:none,_",
  "-opt-inline-from:**",
  "-Yopt-log-inline", "_",
)
javaOptions in Test ++= Seq(
  "-Dlog4j.debug=false",
  "-Dlog4j.configuration=log4j.properties")

javaOptions ++= Seq("-Xms512M", "-Xmx8192M", "-XX:+CMSClassUnloadingEnabled" , "-Dlog4j.configuration=log4j.properties")

//fix for using with hdp warehouse connector
updateOptions := updateOptions.value.withLatestSnapshots(false)
outputStrategy := Some(StdoutOutput)


resolvers ++= Seq(
  "zsibio-snapshots" at "https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/",
  "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven",
  "spring" at "https://repo.spring.io/libs-milestone/",
  "Cloudera" at "https://repository.cloudera.com/content/repositories/releases/",
  "Hortonworks" at "https://repo.hortonworks.com/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

//fix for hdtsdjk patch in hadoop-bam and disq
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("htsjdk.samtools.SAMRecordHelper" -> "htsjdk.samtools.SAMRecordHelperDisq").inLibrary("org.disq-bio" % "disq" % "0.3.6"),
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
  case PathList("jersey", xs@_*) => MergeStrategy.first
  case PathList("scala", xs@_*) => MergeStrategy.first
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
  case "jetty-dir.css" => MergeStrategy.last
  case "module-info.class" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

/* scalastyle setup */
//lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
//compileScalastyle := scalastyle.in(Compile).toTask("").value
//
//scalastyleFailOnError := false
//scalastyleFailOnWarning := false
//
//(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value


credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishTo := {
  if (!version.value.toLowerCase.contains("snapshot")) {
    sonatypePublishToBundle.value
  } else {
    val nexus = "https://zsibio.ii.pw.edu.pl/nexus/repository/"
    Some("snapshots" at nexus + "maven-snapshots")
  }
}