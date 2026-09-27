import XCTest
@testable import Synchro

final class PushLimitsTests: XCTestCase {
    func testCanonicalWriterMatchesServerCanonicalizer() throws {
        let text = (0x00...0x1F).map { String(UnicodeScalar(UInt8($0))) }.joined()
            + "\"\\/\u{7F}\u{2028}\u{2029}é\u{1F600}\u{FFFE}"
        var expectedText = Data(#""\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000b\f\r\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f\"\\/"#.utf8)
        expectedText.append(Data("\u{7F}\u{2028}\u{2029}é\u{1F600}\u{FFFE}\"".utf8))
        XCTAssertEqual(expectedText.count, 195)
        XCTAssertEqual(try Integrity.canonicalJSONValue(text), expectedText)
        XCTAssertEqual(
            try PushLimits.canonicalJSON(JSONEncoder.synchroEncoder().encode(text)),
            expectedText
        )

        let numbers = "[0,-0.0,1,1.5,0.1,1e-7,1e16,123456789012345680,1e21,5e-324,18446744073709552000,9223372036854775807,-2.5e-8,1e-6,999999999999999900000]"
        XCTAssertEqual(
            String(decoding: try PushLimits.canonicalJSON(Data(numbers.utf8)), as: UTF8.self),
            "[0,0,1,1.5,0.1,1e-7,10000000000000000,123456789012345680,1e+21,5e-324,18446744073709552000,9223372036854776000,-2.5e-8,0.000001,999999999999999900000]"
        )
    }

    func testNormalizedMutationMatchesServerForm() throws {
        let mutation = Mutation(
            mutationID: "0f8fad5b-d9cb-469f-a165-70867728950e",
            table: "t_notes",
            op: .update,
            pk: ["f_id": AnyCodable("7c9e6679-7425-40de-944b-e07fc1f90ae7")],
            authoredSchema: SchemaRef(
                version: 3,
                hash: "a97280b716fe0f8a9553ba7c3b31b00dd03f7c7aacf0ff01a703d73182f3df31"
            ),
            baseVersion: "v:42",
            clientVersion: "2026-09-27T10:00:00.000000Z",
            columns: [
                "f_score": AnyCodable(1.5),
                "f_none": AnyCodable(NSNull()),
                "f_flag": AnyCodable(true),
                "f_count": AnyCodable(Int64(7)),
                "f_body": AnyCodable("a/b"),
            ]
        )

        let measure = try PushLimits.measure(mutation, encoder: JSONEncoder.synchroEncoder())

        let expected = #"["mutation-v1","0f8fad5b-d9cb-469f-a165-70867728950e","t_notes",["f_id","7c9e6679-7425-40de-944b-e07fc1f90ae7"],["3","a97280b716fe0f8a9553ba7c3b31b00dd03f7c7aacf0ff01a703d73182f3df31"],"update",[1,"v:42"],"2026-09-27T10:00:00.000000Z",[1,[["f_body","a/b"],["f_count",7],["f_flag",true],["f_none",null],["f_score",1.5]]]]"#
        XCTAssertEqual(measure.normalizedJSON.count, 320)
        XCTAssertEqual(String(decoding: measure.normalizedJSON, as: UTF8.self), expected)
        XCTAssertEqual(measure.authoredColumns, 5)
    }
}
