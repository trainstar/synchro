import UIKit
import React
import React_RCTAppDelegate
import ReactAppDependencyProvider

@main
class AppDelegate: UIResponder, UIApplicationDelegate {
  private(set) var launchOptions: [UIApplication.LaunchOptionsKey: Any]?

  func application(
    _ application: UIApplication,
    didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
  ) -> Bool {
    self.launchOptions = launchOptions
    return true
  }

  func application(
    _ application: UIApplication,
    configurationForConnecting connectingSceneSession: UISceneSession,
    options: UIScene.ConnectionOptions
  ) -> UISceneConfiguration {
    let configuration = UISceneConfiguration(
      name: "Default Configuration",
      sessionRole: connectingSceneSession.role
    )
    configuration.sceneClass = UIWindowScene.self
    configuration.delegateClass = SceneDelegate.self
    return configuration
  }
}

class SceneDelegate: UIResponder, UIWindowSceneDelegate {
  var window: UIWindow?

  var reactNativeDelegate: ReactNativeDelegate?
  var reactNativeFactory: RCTReactNativeFactory?

  func scene(
    _ scene: UIScene,
    willConnectTo session: UISceneSession,
    options connectionOptions: UIScene.ConnectionOptions
  ) {
    guard let windowScene = scene as? UIWindowScene else { return }

    let delegate = ReactNativeDelegate()
    let factory = RCTReactNativeFactory(delegate: delegate)
    delegate.dependencyProvider = RCTAppDependencyProvider()

    reactNativeDelegate = delegate
    reactNativeFactory = factory

    window = UIWindow(windowScene: windowScene)

    let arguments = ProcessInfo.processInfo.arguments
    let conformanceDetox = arguments.indices.contains { index in
      arguments[index] == "-synchroConformance"
        && index + 1 < arguments.endIndex
        && arguments[index + 1] == "1"
    }
    let appDelegate = UIApplication.shared.delegate as! AppDelegate
    factory.startReactNative(
      withModuleName: "SynchroReactNativeExample",
      in: window,
      initialProperties: ["conformanceDetox": conformanceDetox],
      launchOptions: appDelegate.launchOptions
    )
  }
}

class ReactNativeDelegate: RCTDefaultReactNativeFactoryDelegate {
  override func sourceURL(for bridge: RCTBridge) -> URL? {
    self.bundleURL()
  }

  override func bundleURL() -> URL? {
#if DEBUG
#if DETOX_E2E
    Bundle.main.url(forResource: "main", withExtension: "jsbundle")
#else
    RCTBundleURLProvider.sharedSettings().jsBundleURL(forBundleRoot: "index")
#endif
#else
    Bundle.main.url(forResource: "main", withExtension: "jsbundle")
#endif
  }
}
