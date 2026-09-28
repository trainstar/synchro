plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

val synchroVersion = providers.gradleProperty("synchroVersion").orNull
    ?: error("synchroVersion is required")
val upgradeVersionCode = providers.gradleProperty("upgradeVersionCode").orNull?.toInt()
    ?: error("upgradeVersionCode is required")

android {
    namespace = "com.trainstar.synchro.upgrade"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.trainstar.synchro.upgrade"
        minSdk = 24
        targetSdk = 34
        versionCode = upgradeVersionCode
        versionName = synchroVersion
        buildConfigField("String", "SYNCHRO_VERSION", "\"$synchroVersion\"")
    }

    buildFeatures {
        buildConfig = true
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
        isCoreLibraryDesugaringEnabled = true
    }

    kotlinOptions {
        jvmTarget = "1.8"
    }
}

dependencies {
    coreLibraryDesugaring("com.android.tools:desugar_jdk_libs:2.0.4")
    implementation("fit.trainstar:synchro:$synchroVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.0")
}
