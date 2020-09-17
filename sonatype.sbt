// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "org.biodatageeks"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// Open-source license of your choice
licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

// Where is the source code hosted: GitHub or GitLab?
import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("biodatageeks", "sequila", "team@biodatageeks.org"))


// or if you want to set these fields manually
homepage := Some(url("http://biodatageeks.org/sequila/"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/biodatageeks/sequila"),
    "scm:git@github.com:biodatageeks/sequila.git"
  )
)
developers := List(
  Developer(id="biodatageeks", name="biodatageeks", email="team@biodatageeks.org", url=url("http://biodatageeks.org/"))
)

sonatypeLogLevel := "INFO"