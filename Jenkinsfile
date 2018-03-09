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
channel = "#project-genomicranges"


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

  slackSend(bot:true, channel: channel, attachments: attachments.toString())
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
                        sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt publish"


            }

            stage('Code stats') {

               echo 'Gathering code stats....'
               sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt stats"

                                    }
          stage('Readthedocs') {

             echo 'Generating readthedocs....'
             sh "cd docs && make html"
             publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, keepAll: false, reportDir: 'docs/build/html/', reportFiles: 'index.html', reportName: 'Readthedocs', reportTitles: ''])
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
