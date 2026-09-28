pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

// "central" resolves the published predecessor from Maven Central. A path
// resolves the candidate from its consumer repository. Either source is
// exclusive for the Synchro group.
val synchroRepository = providers.environmentVariable("SYNCHRO_UPGRADE_MAVEN_REPOSITORY").orNull
    ?: error("SYNCHRO_UPGRADE_MAVEN_REPOSITORY is required")

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        exclusiveContent {
            forRepository {
                if (synchroRepository == "central") {
                    mavenCentral()
                } else {
                    maven {
                        name = "synchroCandidate"
                        url = uri(synchroRepository)
                    }
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

rootProject.name = "synchro-upgrade-consumer"
include(":app")
