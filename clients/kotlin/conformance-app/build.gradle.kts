plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.trainstar.synchro.conformance"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.trainstar.synchro.conformance"
        minSdk = 24
        targetSdk = 34
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
    coreLibraryDesugaring("com.android.tools:desugar_jdk_libs:2.1.5")
    implementation(project(":synchro"))
    androidTestImplementation("androidx.test.ext:junit:1.3.0")
    androidTestImplementation("androidx.test:core-ktx:1.7.0")
    androidTestImplementation("androidx.test:runner:1.7.0")
    androidTestImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.11.0")
    androidTestImplementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
}
