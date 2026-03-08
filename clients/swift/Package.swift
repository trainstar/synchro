// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "Synchro",
    platforms: [
        .iOS(.v16),
        .macOS(.v13),
    ],
    products: [
        .library(name: "Synchro", targets: ["Synchro"]),
    ],
    dependencies: [
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.0.0"),
    ],
    targets: [
        .target(
            name: "Synchro",
            dependencies: [
                .product(name: "GRDB", package: "GRDB.swift"),
            ],
            path: "Sources/Synchro"
        ),
        .testTarget(
            name: "SynchroTests",
            dependencies: ["Synchro"],
            path: "Tests/SynchroTests"
        ),
    ]
)
