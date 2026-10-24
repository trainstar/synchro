// swift-tools-version: 6.0

import PackageDescription

guard let synchroPackagePath = Context.environment["SYNCHRO_SWIFT_PACKAGE_PATH"],
      !synchroPackagePath.isEmpty
else {
    fatalError("SYNCHRO_SWIFT_PACKAGE_PATH is required")
}

let package = Package(
    name: "SynchroConsumer",
    platforms: [.macOS(.v13)],
    dependencies: [
        .package(path: synchroPackagePath),
    ],
    targets: [
        .executableTarget(
            name: "SynchroConsumer",
            dependencies: [
                .product(name: "Synchro", package: "Synchro"),
            ]
        ),
    ],
    swiftLanguageModes: [.v6]
)
