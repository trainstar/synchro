require "json"

package = JSON.parse(File.read(File.join(__dir__, "package.json")))

Pod::Spec.new do |s|
  s.name         = "SynchroReactNative"
  s.version      = package["version"]
  s.summary      = package["description"]
  s.homepage     = package["homepage"]
  s.license      = package["license"]
  s.authors      = package["author"]

  s.platforms    = { :ios => "17.0" }
  s.source       = { :git => "https://github.com/trainstar/synchro.git", :tag => "v#{s.version}" }

  s.source_files = "ios/**/*.{h,m,mm,swift,cpp}"
  s.private_header_files = "ios/**/*.h"
  s.compiler_flags = "-Werror=protocol"

  s.dependency "Synchro", "= #{s.version}"

  install_modules_dependencies(s)

  # Test sources stay outside source_files, so production targets never compile them.
  s.test_spec "Tests" do |test_spec|
    test_spec.source_files = "ios-tests/**/*.swift"
    test_spec.requires_app_host = true
  end
end
