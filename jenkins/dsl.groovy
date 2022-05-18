multibranchPipelineJob('CKAN-build') {
    branchSources {
      branchSource {
        buildStrategies {
          buildChangeRequests {
            ignoreTargetOnlyChanges(true)
            ignoreUntrustedChanges(false)
          }
          buildTags {
            atMostDays('7')
            atLeastDays(null)
          }
        }

        source {
          github {
              id('ckan_project_template')
              credentialsId('jenkins_github_api')
              repoOwner('Fjelltopp')
              repository('ckan_project_template')
              repositoryUrl("https://github.com/fjelltopp/ckan_project_template.git")
              configuredByUrl(true)
              traits {
                gitHubTagDiscovery()
                ignoreDraftPullRequestFilterTrait()
                gitHubSshCheckout {
                  credentialsId('jenkins_github_ssh')
                }
                submoduleOptionTrait {
                  extension {
                    disableSubmodules(false)
                    recursiveSubmodules(true)
                    trackingSubmodules(false)
                    reference(null)
                    timeout(null)
                    parentCredentials(true)
                  }
                }
              }

          }
        }
      }
    }

    orphanedItemStrategy {
        discardOldItems {
            numToKeep(30)
            daysToKeep(14)
        }
    }
    factory {
        workflowBranchProjectFactory {
            scriptPath('jenkins/Jenkinsfile.build.groovy')
        }
    }

    configure {
        def branchsource = 'jenkins.branch.BranchSource'
        def traits = it / sources / data / branchsource / source / traits
        traits << "org.jenkinsci.plugins.github_branch_source.OriginPullRequestDiscoveryTrait" {
            strategyId(1);
        };
    }
}


pipelineJob("CKAN-deploy") {
  properties {
    disableConcurrentBuilds()
  }

  definition {
    logRotator {
        daysToKeep(30)
        numToKeep(30)
    }
    cpsScm {
      scm {
        git {
          remote {
            url('git@github.com:fjelltopp/ckan_project_template.git')
            credentials('jenkins_github_ssh')
            name('engine')
          }
          remote {
            url('git@github.com:fjelltopp/fjelltopp-infrastructure.git')
            credentials('jenkins_github_ssh')
            name('origin')
          }
          scriptPath('jenkinsfiles/ckan_deploy.groovy')
          branch("remotes/origin/master")
        }
      }
    }
  }
}

