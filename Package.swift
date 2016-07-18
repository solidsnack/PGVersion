import PackageDescription


let v3alpha = Version(3, 0, 0, prereleaseIdentifiers: ["alpha.1"])


let package = Package(
    name: "PGVersion",
    dependencies: [
        .Package(url: "https://github.com/solidsnack/CLibPQ.git",
                 majorVersion: 1),
        .Package(url: "https://github.com/antitypical/Result.git",
                 versions: v3alpha..<Version(3, .max, .max))
    ]
)
