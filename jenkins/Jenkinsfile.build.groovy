def checkout_scm() {
  script {
    checkout scm
  }
}

def build_docker_image(name, tag) {
  echo "Building container image"

  script {
    def revision = "${tag}"
    def repository = "https://${env.CKAN_ECR_REGISTRY}"
    docker.withRegistry(repository) {
      def image = docker.build("${name}:${revision}", "-f Dockerfile --progress=plain .")
      image.push()
    }
  }
}

def tag = "${env.BRANCH_NAME}".toString().replaceAll("/", "_slash_")


pipeline {

  options {
    disableConcurrentBuilds()
  }

  agent any

  stages {
    stage('checkout_scm') {
      steps {
        checkout_scm()
        }
    }
    stage('Login to ECR'){
      steps{
        withAWS(roleAccount:'074027416471', role:'Fjelltopp-cross-account-role') {
          sh "aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin ${env.CKAN_ECR_REGISTRY}"
        }
      }
    }
    stage('Build Image') {
      steps {
        build_docker_image('ckan-fjelltopp', tag)
        }

    }
  }
  post {
      always {
          cleanWs()
      }
/*      success {
        slackSend (color: '#00FF00', message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
      }*/

      failure {
        slackSend (color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
      }
  }
}
