Pod::Spec.new do |s|
  s.name         = "Synchro"
  s.version      = "0.2.0"
  s.summary      = "Offline-first sync SDK for iOS"
  s.homepage     = "https://github.com/trainstar/synchro"
  s.license      = { :type => "MIT" }
  s.author       = "Trainstar"
  s.source       = { :git => "https://github.com/trainstar/synchro.git", :tag => s.version }
  s.ios.deployment_target = "16.0"
  s.swift_version = "5.9"
  s.source_files = "Sources/Synchro/**/*.swift"
  s.dependency "GRDB.swift", "~> 7.0"
end
