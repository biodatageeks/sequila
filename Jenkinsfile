pipeline {
    agent any
       stages {
        stage('Test Scala code') {
            steps {
                slackSend botUser: true, channel: '#project-genomicranges', message: "started ${env.JOB_NAME} ${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)", teamDomain: 'zsibio.slack.com'
                echo 'Testing Scala code....'
                sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt test"
            }
            post {
              always {
                junit '**/target/test-reports/*.xml'
              }
            }
        }

        stage('Package scala code') {
                    steps {
                        echo 'Building Scala code....'
                        sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt package"
			            echo "Generating documentation"
			            sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt doc"

                    }

        }
        stage('Publish to Nexus snapshots') {
            steps {
                    echo "branch: ${env.BRANCH_NAME}"
                    echo 'Publishing to ZSI-BIO snapshots repository....'
                    sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt publish"


            }
        }
    }
}
