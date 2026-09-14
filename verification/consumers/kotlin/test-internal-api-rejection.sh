#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <gradle-wrapper> <synchro-version>" >&2
    exit 2
fi

gradle_wrapper="$1"
synchro_version="$2"

: "${SYNCHRO_CONSUMER_MAVEN_REPOSITORY:?SYNCHRO_CONSUMER_MAVEN_REPOSITORY is required}"
: "${ANDROID_HOME:?ANDROID_HOME is required}"
: "${JAVA_HOME:?JAVA_HOME is required}"

temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/synchro-internal-api.XXXXXX")"
cleanup() {
    rm -rf "$temporary_root"
}
trap cleanup EXIT HUP INT TERM

write_project() {
    project="$1"
    mkdir -p "$project/app/src/main/kotlin/probe" "$project/app/src/main/java/probe"
    cat > "$project/app/src/main/AndroidManifest.xml" <<'EOF'
<manifest xmlns:android="http://schemas.android.com/apk/res/android" />
EOF
    cat > "$project/settings.gradle.kts" <<EOF
pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        maven { url = uri("$SYNCHRO_CONSUMER_MAVEN_REPOSITORY") }
        google()
        mavenCentral()
    }
}

rootProject.name = "synchro-internal-api-rejection"
include(":app")
EOF
    cat > "$project/build.gradle.kts" <<'EOF'
plugins {
    id("com.android.application") version "8.2.2" apply false
    id("org.jetbrains.kotlin.android") version "1.9.22" apply false
}
EOF
    cat > "$project/app/build.gradle.kts" <<EOF
plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.trainstar.synchro.internalprobe"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.trainstar.synchro.internalprobe"
        minSdk = 24
        targetSdk = 34
        versionCode = 1
        versionName = "1.0"
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
    implementation("fit.trainstar:synchro:$synchro_version")
}
EOF
}

assert_rejected() {
    name="$1"
    project="$2"
    task="$3"
    logfile="$temporary_root/$name.log"
    if "$gradle_wrapper" --project-dir "$project" --no-daemon "$task" >"$logfile" 2>&1; then
        echo "$name unexpectedly compiled against Synchro internals" >&2
        exit 1
    fi
    if ! grep -F "SynchroDatabase" "$logfile" >/dev/null || ! grep -F "SynchroMeta" "$logfile" >/dev/null; then
        echo "$name failed before the internal API rejection check" >&2
        exit 1
    fi
}

java_project="$temporary_root/java"
write_project "$java_project"
cat > "$java_project/app/src/main/java/probe/ForbiddenJavaAccess.java" <<'EOF'
package probe;

import com.trainstar.synchro.SynchroDatabase;
import com.trainstar.synchro.SynchroMeta;

final class ForbiddenJavaAccess {
    void access() {
        SynchroDatabase database = new SynchroDatabase(null, "forbidden.db");
        database.getWritableDatabase();
        SynchroMeta.INSTANCE.set(null, null, "1");
    }
}
EOF
assert_rejected "java" "$java_project" ":app:compileDebugJavaWithJavac"

kotlin_project="$temporary_root/kotlin"
write_project "$kotlin_project"
cat > "$kotlin_project/app/src/main/kotlin/probe/ForbiddenKotlinAccess.kt" <<'EOF'
package probe

import com.trainstar.synchro.SynchroDatabase
import com.trainstar.synchro.SynchroMeta

fun forbiddenAccess() {
    val database = SynchroDatabase.open(null!!, "forbidden.db")
    SynchroMeta.set(database.readTransaction { it }, error("unreachable"), "1")
}
EOF
assert_rejected "kotlin" "$kotlin_project" ":app:compileDebugKotlin"
