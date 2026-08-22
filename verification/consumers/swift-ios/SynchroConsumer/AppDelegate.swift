import Synchro
import UIKit

@main
final class AppDelegate: UIResponder, UIApplicationDelegate {
    var window: UIWindow?
    private var client: SynchroClient?

    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        let window = UIWindow(frame: UIScreen.main.bounds)
        let viewController = UIViewController()
        viewController.view.backgroundColor = .systemBackground
        window.rootViewController = viewController
        window.makeKeyAndVisible()
        self.window = window

        do {
            let documents = try FileManager.default.url(
                for: .documentDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: true
            )
            let databaseURL = documents.appendingPathComponent("consumer.db")
            let config = SynchroConfig(
                dbPath: databaseURL.path,
                serverURL: URL(string: "http://127.0.0.1")!,
                authProvider: { "unused" },
                clientID: "packaged-ios-consumer",
                platform: "ios",
                appVersion: "consumer"
            )
            let client = try SynchroClient(config: config)
            try client.execute(
                "CREATE TABLE IF NOT EXISTS consumer_probe (id TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            try client.execute(
                "INSERT OR REPLACE INTO consumer_probe (id, value) VALUES (?, ?)",
                params: ["probe", "packaged"]
            )
            self.client = client
        } catch {
            fatalError("Packaged Synchro probe failed: \(error)")
        }

        return true
    }
}
