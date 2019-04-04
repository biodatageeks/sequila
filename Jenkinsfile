#!/usr/bin/env groovy

import groovy.json.JsonOutput
import java.util.Optional
import hudson.tasks.test.AbstractTestResultAction
import hudson.model.Actionable
import hudson.tasks.junit.CaseResult
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

author = ""
message = ""
channel = "#project-sequila"


def getGitAuthor = {
    def commit = sh(returnStdout: true, script: 'git rev-parse HEAD')
    author = sh(returnStdout: true, script: "git --no-pager show -s --format='%an' ${commit}").trim()
}

def getLastCommitMessage = {
    message = sh(returnStdout: true, script: 'git log -1 --pretty=%B').trim()
}


def populateGlobalVariables = {
    getLastCommitMessage()
    getGitAuthor()
    println author
    println message
}



def notifySlack = {
    JSONArray attachments = new JSONArray();
    JSONObject attachment = new JSONObject();
    JSONArray fields = new JSONArray();


    JSONObject authorField = new JSONObject();
    authorField.put("title","Author")
    authorField.put("value",author)

    JSONObject msgField = new JSONObject();
    msgField.put("title","Last commit")
    msgField.put("value",message)

    JSONObject jobField = new JSONObject();
    jobField.put("title","Job name")
    jobField.put("value",env.JOB_NAME)

    JSONObject linkField = new JSONObject();
    linkField.put("title","Jenkins link")
    linkField.put("value",env.BUILD_URL.replace("http://","http://www."))



    JSONObject statusField = new JSONObject();
    statusField.put("title","Build status")
    statusField.put("value",buildStatus)


    fields.add(jobField)
    fields.add(authorField)
    fields.add(msgField)
    fields.add(linkField)
    fields.add(statusField)





    attachment.put('text',"Another great piece of code has been tested...");
    attachment.put('fallback','Hey, Vader seems to be mad at you.');
    attachment.put("fields",fields)
    attachment.put('color',buildColor);


    attachments.add(attachment);

  slackSend(bot:false, channel: channel, attachments: attachments.toString())
}



node {
 stage('Checkout') {
            checkout scm
        }

    populateGlobalVariables()
 try {
           stage('Test Scala code') {

                    echo 'Testing Scala code....'
                    sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt clean test"




            }

            stage('Package scala code') {

                            echo 'Building Scala code....'
                            sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt package"
    			            echo "Generating documentation"
    			            sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt doc"
    			            //publishHTML([allowMissing: false, alwaysLinkToLastBuild: true, keepAll: false, reportDir: 'target/scala-2.11/api/', reportFiles: 'package.html', reportName: 'Scala Doc', reportTitles: ''])

                        }


            stage('Publish to Nexus snapshots and dopy DAGs') {

                        echo "branch: ${env.BRANCH_NAME}"
                        echo 'Publishing to ZSI-BIO snapshots repository....'
                        sh "SBT_OPTS='-XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=2G -Xmx2G' ${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt publish"


            }

            stage('Code stats') {

               echo 'Gathering code stats....'
               sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt stats"

                                    }
          stage('Readthedocs') {

             echo 'Generating readthedocs....'

             publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, keepAll: false, reportDir: 'docs/build/html/', reportFiles: 'index.html', reportName: 'Readthedocs', reportTitles: ''])
             sh './build_docs.sh'
          }

           stage('Building Docker images') {

                     echo 'Building Docker images....'
                     //sh './build.sh'

                     }

           stage('Performance testing') {
                sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt -J-Xms2048m -J-Xmx2048m 'set test in assembly := {}' assembly"
                sh "ssh bdg-perf@cdh00 rm -rf /tmp/bdg-sequila-assembly-*.jar"
                sh "scp target/scala-2.11/bdg-sequila-assembly-*.jar bdg-perf@cdh00:/tmp"
                sh "scp performance/bdg_perf/bdg_perf_sequila.scala bdg-perf@cdh00:/tmp"
                sh 'ssh bdg-perf@cdh00 ". ~/.profile; spark-shell  --conf spark.sql.catalogImplementation=in-memory --conf spark.hadoop.yarn.timeline-service.enabled=false --principal bdg-perf@CL.II.PW.EDU.PL --keytab /data/work/home/bdg-perf/keytabs/bdg-perf.keytab --master=yarn-client --executor-memory=3g --num-executors=40 --executor-cores=1 --driver-memory=8g -i /tmp/bdg_perf_sequila.scala --packages org.postgresql:postgresql:42.1.1 --conf spark.biodatageeks.perf.testId=$BRANCH_NAME --jars /tmp/bdg-sequila-assembly-*.jar -v"'
                sh './build_perf_report.sh'
                 }



 }
 catch (e){currentBuild.result="FAIL"}
 stage('Notify'){
    junit '**/target/test-reports/*.xml'
    buildColor = currentBuild.result == null ? "good" : "danger"
    buildStatus = currentBuild.result == null ? "SUCCESS:clap:" : currentBuild.result+":cry:"

    notifySlack()
    }


}
