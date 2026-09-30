// swift-tools-version: 5.9

import PackageDescription

// One application source builds against either the published predecessor
// release or the candidate package artifact.
let synchro: Package.Dependency
let synchroIdentity: String
if let version = Context.environment["SYNCHRO_UPGRADE_SWIFT_RELEASE"], !version.isEmpty {
    synchro = .package(url: "https://github.com/trainstar/synchro.git", exact: Version(stringLiteral: version))
    synchroIdentity = "synchro"
} else if let path = Context.environment["SYNCHRO_SWIFT_PACKAGE_PATH"], !path.isEmpty {
    synchro = .package(path: path)
    synchroIdentity = "Synchro"
} else {
    fatalError("SYNCHRO_UPGRADE_SWIFT_RELEASE or SYNCHRO_SWIFT_PACKAGE_PATH is required")
}

let package = Package(
    name: "SynchroUpgrade",
    platforms: [.macOS(.v13)],
    dependencies: [synchro],
    targets: [
        .executableTarget(
            name: "SynchroUpgrade",
            dependencies: [.product(name: "Synchro", package: synchroIdentity)]
        ),
    ]
)
