pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

val synchroRepository = providers.environmentVariable("SYNCHRO_CONSUMER_MAVEN_REPOSITORY").orNull
    ?: error("SYNCHRO_CONSUMER_MAVEN_REPOSITORY is required")

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        exclusiveContent {
            forRepository {
                maven {
                    name = "synchroCandidate"
                    url = uri(synchroRepository)
                }
            }
            filter {
                includeGroup("fit.trainstar")
            }
        }
        google()
        mavenCentral()
    }
}

rootProject.name = "synchro-packaged-consumer"
include(":app")
