plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

val synchroVersion = providers.gradleProperty("synchroVersion").orNull
    ?: error("synchroVersion is required")

android {
    namespace = "com.trainstar.synchro.consumer"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.trainstar.synchro.consumer"
        minSdk = 24
        targetSdk = 34
        versionCode = 1
        versionName = "1.0"
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
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

    androidTestImplementation("androidx.test:core:1.5.0")
    androidTestImplementation("androidx.test.ext:junit:1.1.5")
    androidTestImplementation("androidx.test:runner:1.5.2")
}
