Pod::Spec.new do |s|
  s.name = "Synchro"
  s.version = "0.2.0"
  s.summary = "Offline-first sync SDK for Apple platforms"
  s.homepage = "https://github.com/trainstar/synchro"
  s.license = { :type => "MIT" }
  s.author = "Trainstar"
  s.source = { :git => "https://github.com/trainstar/synchro.git", :tag => "v#{s.version}" }
  s.ios.deployment_target = "16.0"
  s.osx.deployment_target = "13.0"
  s.swift_version = "5.9"
  s.source_files = "clients/swift/Sources/Synchro/**/*.swift"
  s.dependency "GRDB.swift"
end
