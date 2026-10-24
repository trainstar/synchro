import re
import sys
from pathlib import Path


def replace_exactly_once(path: Path, pattern: str, replacement: str, label: str) -> None:
    content = path.read_text()
    updated, count = re.subn(pattern, replacement, content, count=1, flags=re.MULTILINE)
    if count != 1:
        raise SystemExit(f"expected one {label} entry in {path}")
    path.write_text(updated)


if len(sys.argv) != 3:
    raise SystemExit("usage: prepare-android-host.py <android directory> <React Native version>")

android_dir = Path(sys.argv[1])
rn_version = sys.argv[2]
build_file = android_dir / "build.gradle"
app_build_file = android_dir / "app" / "build.gradle"
wrapper_file = android_dir / "gradle" / "wrapper" / "gradle-wrapper.properties"
if not build_file.is_file() or not app_build_file.is_file() or not wrapper_file.is_file():
    raise SystemExit("generated React Native Android host files are missing")

for name, value in (
    ("compileSdkVersion", "37"),
    ("targetSdkVersion", "37"),
    ("kotlinVersion", '"2.4.21"'),
):
    replace_exactly_once(
        build_file,
        rf"^(\s*{name}\s*=\s*)[^\r\n]+$",
        rf"\g<1>{value}",
        name,
    )

replace_exactly_once(
    build_file,
    r'^(\s*classpath\(["\'])com\.android\.tools\.build:gradle(?::[^"\']+)?(["\']\)\s*)$',
    r'\g<1>com.android.tools.build:gradle:9.4.1\g<2>',
    "Android Gradle Plugin classpath",
)
replace_exactly_once(
    build_file,
    r'^(\s*classpath\(["\'])org\.jetbrains\.kotlin:kotlin-gradle-plugin(?::[^"\']+)?(["\']\)\s*)$',
    r'\g<1>org.jetbrains.kotlin:kotlin-gradle-plugin:2.4.21\g<2>',
    "Kotlin Gradle Plugin classpath",
)

wrapper = wrapper_file.read_text()
distribution = r"^distributionUrl=https\\://services\.gradle\.org/distributions/gradle-[0-9.]+-(?:bin|all)\.zip$"
wrapper, count = re.subn(
    distribution,
    "distributionUrl=https\\://services.gradle.org/distributions/gradle-9.8.2-bin.zip",
    wrapper,
    count=1,
    flags=re.MULTILINE,
)
if count != 1:
    raise SystemExit(f"expected one Gradle distribution entry in {wrapper_file}")
checksum = r"^distributionSha256Sum=[0-9a-f]{64}$"
if re.search(r"^distributionSha256Sum=", wrapper, flags=re.MULTILINE):
    wrapper, count = re.subn(
        checksum,
        "distributionSha256Sum=21bd5da2de49b642904077f38c0efdc7778e92b3ed0889392dca61b97e389607",
        wrapper,
        count=1,
        flags=re.MULTILINE,
    )
    if count != 1:
        raise SystemExit(f"expected one valid Gradle checksum entry in {wrapper_file}")
else:
    wrapper = wrapper.replace(
        "distributionUrl=https\\://services.gradle.org/distributions/gradle-9.8.2-bin.zip\n",
        "distributionUrl=https\\://services.gradle.org/distributions/gradle-9.8.2-bin.zip\n"
        "distributionSha256Sum=21bd5da2de49b642904077f38c0efdc7778e92b3ed0889392dca61b97e389607\n",
        1,
    )
wrapper_file.write_text(wrapper)

if rn_version == "0.82.1":
    app_build = app_build_file.read_text()
    app_build, count = re.subn(
        r'getDefaultProguardFile\("proguard-android\.txt"\)',
        'getDefaultProguardFile("proguard-android-optimize.txt")',
        app_build,
        count=1,
    )
    if count != 1:
        raise SystemExit("React Native 0.82.1 app build has no expected ProGuard default")
    app_build, count = re.subn(
        r'^android \{\n',
        'android {\n    buildFeatures { resValues = true }\n',
        app_build,
        flags=re.MULTILINE,
    )
    if count != 1:
        raise SystemExit("React Native 0.82.1 app build has no unique Android configuration")
    app_build_file.write_text(app_build)
    properties_file = android_dir / "gradle.properties"
    properties = properties_file.read_text()
    if re.search(r'^android\.(builtInKotlin|newDsl)\s*=', properties, flags=re.MULTILINE):
        raise SystemExit("React Native 0.82.1 template has unexpected Android plugin settings")
    properties_file.write_text(
        properties.rstrip() + "\nandroid.builtInKotlin=false\nandroid.newDsl=false\n"
    )
elif rn_version != "0.88.0":
    raise SystemExit(f"unsupported React Native version: {rn_version}")
