"""Generate type_zoo rows for PG type round-trip validation.

7 rows covering typical values, NULLs, min/max boundaries, unicode,
edge cases, and empty-vs-NULL distinctions.
"""

from tpch_transform import deterministic_uuid, sql_str


def generate_type_zoo(user_ids: list[str]) -> list[str]:
    """Generate type_zoo INSERT statements."""
    stmts = []

    rows = [
        # Row 1: typical values
        {
            "id": deterministic_uuid("typezoo", "1"),
            "user_id": user_ids[0 % len(user_ids)],
            "col_text": "'hello world'",
            "col_varchar": "'varchar value'",
            "col_char": "'CHAR10    '",
            "col_boolean": "true",
            "col_smallint": "32767",
            "col_integer": "2147483647",
            "col_bigint": "9223372036854775807",
            "col_numeric": "123456.789012",
            "col_real": "3.14",
            "col_double": "2.718281828459045",
            "col_date": "'2025-06-15'",
            "col_timestamptz": "'2025-06-15T12:30:00Z'",
            "col_timestamp": "'2025-06-15 12:30:00'",
            "col_interval": "'3 days 4 hours'",
            "col_jsonb": "'{\"key\": \"value\", \"nested\": {\"a\": 1}}'::jsonb",
            "col_json": "'{\"simple\": true}'::json",
            "col_text_array": "ARRAY['tag1', 'tag2', 'tag3']",
            "col_int_array": "ARRAY[1, 2, 3]",
            "col_bytea": "'\\xDEADBEEF'",
            "col_inet": "'192.168.1.1'",
            "col_cidr": "'10.0.0.0/8'",
            "col_macaddr": "'08:00:2b:01:02:03'",
            "col_uuid": "'a1b2c3d4-e5f6-7890-abcd-ef1234567890'",
            "col_point": "'(1.5, 2.5)'",
            "col_int4range": "'[1, 10)'",
            "col_tstzrange": "'[2025-01-01T00:00:00Z, 2025-12-31T23:59:59Z)'",
            "col_xml": "'<root><item>test</item></root>'",
        },
        # Row 2: NULLs for every nullable column
        {
            "id": deterministic_uuid("typezoo", "2"),
            "user_id": user_ids[1 % len(user_ids)],
            "col_text": "NULL", "col_varchar": "NULL", "col_char": "NULL",
            "col_boolean": "NULL", "col_smallint": "NULL", "col_integer": "NULL",
            "col_bigint": "NULL", "col_numeric": "NULL", "col_real": "NULL",
            "col_double": "NULL", "col_date": "NULL", "col_timestamptz": "NULL",
            "col_timestamp": "NULL", "col_interval": "NULL", "col_jsonb": "NULL",
            "col_json": "NULL", "col_text_array": "NULL", "col_int_array": "NULL",
            "col_bytea": "NULL", "col_inet": "NULL", "col_cidr": "NULL",
            "col_macaddr": "NULL", "col_uuid": "NULL", "col_point": "NULL",
            "col_int4range": "NULL", "col_tstzrange": "NULL", "col_xml": "NULL",
        },
        # Row 3: minimum / zero / empty values
        {
            "id": deterministic_uuid("typezoo", "3"),
            "user_id": user_ids[2 % len(user_ids)],
            "col_text": "''",
            "col_varchar": "''",
            "col_char": "NULL",
            "col_boolean": "false",
            "col_smallint": "-32768",
            "col_integer": "-2147483648",
            "col_bigint": "-9223372036854775808",
            "col_numeric": "-0.000001",
            "col_real": "0.0",
            "col_double": "0.0",
            "col_date": "'1970-01-01'",
            "col_timestamptz": "'1970-01-01T00:00:00Z'",
            "col_timestamp": "'1970-01-01 00:00:00'",
            "col_interval": "'0 seconds'",
            "col_jsonb": "'[]'::jsonb",
            "col_json": "'[]'::json",
            "col_text_array": "'{}'::text[]",
            "col_int_array": "'{}'::integer[]",
            "col_bytea": "'\\x'",
            "col_inet": "'::1'",
            "col_cidr": "'::0/0'",
            "col_macaddr": "'00:00:00:00:00:00'",
            "col_uuid": "'00000000-0000-0000-0000-000000000000'",
            "col_point": "'(0, 0)'",
            "col_int4range": "'empty'",
            "col_tstzrange": "'empty'",
            "col_xml": "NULL",
        },
        # Row 4: maximum values
        {
            "id": deterministic_uuid("typezoo", "4"),
            "user_id": user_ids[0 % len(user_ids)],
            "col_text": "NULL",
            "col_varchar": "NULL",
            "col_char": "NULL",
            "col_boolean": "NULL",
            "col_smallint": "32767",
            "col_integer": "2147483647",
            "col_bigint": "9223372036854775807",
            "col_numeric": "99999999999999.999999",
            "col_real": "3.4028235e+38",
            "col_double": "1.7976931348623157e+308",
            "col_date": "'9999-12-31'",
            "col_timestamptz": "'9999-12-31T23:59:59Z'",
            "col_timestamp": "'9999-12-31 23:59:59'",
            "col_interval": "'999999 hours'",
            "col_jsonb": "'{\"deeply\": {\"nested\": {\"object\": {\"with\": {\"many\": \"levels\"}}}}}'::jsonb",
            "col_json": "NULL",
            "col_text_array": "NULL",
            "col_int_array": "ARRAY[-2147483648, 0, 2147483647]",
            "col_bytea": "NULL",
            "col_inet": "'255.255.255.255'",
            "col_cidr": "'192.168.0.0/16'",
            "col_macaddr": "'ff:ff:ff:ff:ff:ff'",
            "col_uuid": "'ffffffff-ffff-ffff-ffff-ffffffffffff'",
            "col_point": "'(-180, 90)'",
            "col_int4range": "'[-2147483648, 2147483647)'",
            "col_tstzrange": "NULL",
            "col_xml": "NULL",
        },
        # Row 5: unicode and special characters
        {
            "id": deterministic_uuid("typezoo", "5"),
            "user_id": user_ids[1 % len(user_ids)],
            "col_text": "E'caf\\u00e9 \\U0001F600 \\t\\n newline'",
            "col_varchar": "'O''Brien & \"quoted\"'",
            "col_char": "NULL",
            "col_boolean": "NULL",
            "col_smallint": "NULL", "col_integer": "NULL", "col_bigint": "NULL",
            "col_numeric": "NULL", "col_real": "NULL", "col_double": "NULL",
            "col_date": "NULL", "col_timestamptz": "NULL", "col_timestamp": "NULL",
            "col_interval": "NULL",
            "col_jsonb": "'{\"emoji\": \"\\ud83d\\ude00\", \"japanese\": \"\\u6771\\u4eac\"}'::jsonb",
            "col_json": "NULL",
            "col_text_array": "ARRAY['has space', 'has,comma', 'has\"quote']",
            "col_int_array": "NULL",
            "col_bytea": "NULL", "col_inet": "NULL", "col_cidr": "NULL",
            "col_macaddr": "NULL", "col_uuid": "NULL", "col_point": "NULL",
            "col_int4range": "NULL", "col_tstzrange": "NULL", "col_xml": "NULL",
        },
        # Row 6: empty vs NULL distinction
        {
            "id": deterministic_uuid("typezoo", "6"),
            "user_id": user_ids[2 % len(user_ids)],
            "col_text": "''",  # empty string, NOT null
            "col_varchar": "''",
            "col_char": "NULL",  # NULL (different from empty)
            "col_boolean": "NULL",
            "col_smallint": "0",  # zero, NOT null
            "col_integer": "0",
            "col_bigint": "0",
            "col_numeric": "0.000000",
            "col_real": "0.0",
            "col_double": "0.0",
            "col_date": "NULL",
            "col_timestamptz": "NULL",
            "col_timestamp": "NULL",
            "col_interval": "NULL",
            "col_jsonb": "'{}'::jsonb",  # empty object, NOT null
            "col_json": "'{}'::json",
            "col_text_array": "'{}'::text[]",  # empty array, NOT null
            "col_int_array": "'{}'::integer[]",
            "col_bytea": "'\\x'",  # empty bytes, NOT null
            "col_inet": "NULL",
            "col_cidr": "NULL",
            "col_macaddr": "NULL",
            "col_uuid": "NULL",
            "col_point": "NULL",
            "col_int4range": "NULL",
            "col_tstzrange": "NULL",
            "col_xml": "NULL",
        },
        # Row 7: large JSONB, long text, large arrays
        {
            "id": deterministic_uuid("typezoo", "7"),
            "user_id": user_ids[0 % len(user_ids)],
            "col_text": "'" + "x" * 10000 + "'",
            "col_varchar": "'" + "y" * 255 + "'",
            "col_char": "NULL",
            "col_boolean": "true",
            "col_smallint": "NULL", "col_integer": "NULL", "col_bigint": "NULL",
            "col_numeric": "NULL", "col_real": "NULL", "col_double": "NULL",
            "col_date": "NULL", "col_timestamptz": "NULL", "col_timestamp": "NULL",
            "col_interval": "NULL",
            "col_jsonb": "'" + '{"items": [' + ",".join(f'{{"i": {i}}}' for i in range(100)) + "]}'::jsonb",
            "col_json": "NULL",
            "col_text_array": "ARRAY[" + ",".join(f"'item_{i}'" for i in range(50)) + "]",
            "col_int_array": "ARRAY[" + ",".join(str(i) for i in range(50)) + "]",
            "col_bytea": "NULL",
            "col_inet": "NULL", "col_cidr": "NULL", "col_macaddr": "NULL",
            "col_uuid": "NULL", "col_point": "NULL",
            "col_int4range": "NULL", "col_tstzrange": "NULL", "col_xml": "NULL",
        },
    ]

    columns = [
        "id", "user_id",
        "col_text", "col_varchar", "col_char",
        "col_boolean", "col_smallint", "col_integer", "col_bigint",
        "col_numeric", "col_real", "col_double",
        "col_date", "col_timestamptz", "col_timestamp", "col_interval",
        "col_jsonb", "col_json",
        "col_text_array", "col_int_array",
        "col_bytea", "col_inet", "col_cidr", "col_macaddr", "col_uuid",
        "col_point", "col_int4range", "col_tstzrange", "col_xml",
    ]

    for row in rows:
        vals = []
        for col in columns:
            if col == "id":
                vals.append(sql_str(row["id"]))
            elif col == "user_id":
                vals.append(sql_str(row["user_id"]))
            else:
                vals.append(row.get(col, "NULL"))
        col_list = ", ".join(columns)
        val_list = ", ".join(vals)
        stmts.append(f"INSERT INTO type_zoo ({col_list}) VALUES ({val_list});")

    return stmts
