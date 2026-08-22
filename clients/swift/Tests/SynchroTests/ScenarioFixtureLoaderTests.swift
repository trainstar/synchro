import XCTest

final class ScenarioFixtureLoaderTests: XCTestCase {
    func testLoadsAuthoredScenarioByCatalogID() throws {
        let scenario = try ScenarioFixtureLoader.load(id: "SCN-SCHEMA-QUEUED-MUTATION-001")

        XCTAssertEqual(scenario["id"] as? String, "SCN-SCHEMA-QUEUED-MUTATION-001")
        XCTAssertEqual(scenario["schema_version"] as? Int, 2)
        XCTAssertEqual(scenario["proof_types"] as? [String], ["server-black-box", "native-e2e", "fault-injection", "negative-control"])
    }

    func testRejectsUnknownScenarioID() {
        XCTAssertThrowsError(try ScenarioFixtureLoader.load(id: "SCN-NOT-AUTHORED-001"))
    }
}
