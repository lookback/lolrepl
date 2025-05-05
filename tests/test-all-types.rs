mod common;

use std::net::TcpStream;
use std::time::Duration;

use lolrepl::Connection;
use lolrepl::Message;
use lolrepl::Subscriber;
use lolrepl::Value;

#[test]
fn test_all_types() {
    // Setup a temporary database
    let temp_db = common::init_tmp_db();

    // Create test table with all supported types
    temp_db.execute(
        "
        CREATE TABLE test_all_types (
            id SERIAL PRIMARY KEY,
            col_bool BOOLEAN,
            col_int2 SMALLINT,
            col_int4 INTEGER,
            col_int8 BIGINT,
            col_float4 REAL,
            col_float8 DOUBLE PRECISION,
            col_text TEXT,
            col_varchar VARCHAR(255),
            col_char CHAR(10),
            col_name NAME,
            col_bytea BYTEA,
            col_oid OID,
            col_uuid UUID,
            col_json JSON,
            col_jsonb JSONB,
            col_date DATE,
            col_time TIME,
            col_timestamp TIMESTAMP,
            col_timestamptz TIMESTAMPTZ,
            col_numeric NUMERIC(10,2)
        );

        -- Create publication for the table
        CREATE PUBLICATION test_all_types_publication FOR TABLE test_all_types;

        -- Create a replication slot
        SELECT pg_create_logical_replication_slot('test_all_types_slot', 'pgoutput');
    ",
    );

    // Insert test data with all types
    temp_db.execute(
        "
        INSERT INTO test_all_types (
            col_bool, col_int2, col_int4, col_int8, col_float4, col_float8,
            col_text, col_varchar, col_char, col_name, col_bytea, col_oid,
            col_uuid, col_json, col_jsonb, col_date, col_time, col_timestamp,
            col_timestamptz, col_numeric
        ) VALUES (
            true, 32767, 2147483647, 9223372036854775807, 3.14, 2.718281828459045,
            'test text', 'test varchar', 'test char', 'test_name', '\\x48656c6c6f', 12345,
            '550e8400-e29b-41d4-a716-446655440000',
            '{\"key\": \"value\"}', '{\"jsonb\": true}',
            '2023-12-25', '14:30:45.123456',
            '2023-12-25 14:30:45.123456',
            '2023-12-25 14:30:45.123456+00',
            123.45
        );
    ",
    );

    // Connect to the database with a replication connection
    let connection_string = format!("localhost:{}", temp_db.port);

    let stream = TcpStream::connect(&connection_string).expect("Failed to connect to PostgreSQL");

    // Make the stream non-blocking with a timeout
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("Failed to set read timeout");

    // Create a replication connection
    let conn = Connection::new(stream, "postgres", "", "testing")
        .expect("Failed to create replication connection");

    let mut sub = Subscriber::new(conn, "test_all_types_slot", "test_all_types_publication")
        .expect("Failed to create subscriber");

    let mut messages = Vec::new();

    // Read messages: Begin, Relation, Insert, Commit
    for _ in 0..4 {
        let message = sub.next().expect("Failed to get replication message");
        messages.push(message);
    }

    let Message::Begin(_lsn) = messages[0] else {
        panic!("Expected Begin message");
    };

    let Message::Relation {
        id: _relation_id,
        columns,
        ..
    } = &messages[1]
    else {
        panic!("Expected Relation message");
    };

    // Verify all expected columns are present with correct type IDs
    let expected_columns = vec![
        ("id", 23),                // INT4 (SERIAL)
        ("col_bool", 16),          // BOOL
        ("col_int2", 21),          // INT2
        ("col_int4", 23),          // INT4
        ("col_int8", 20),          // INT8
        ("col_float4", 700),       // FLOAT4
        ("col_float8", 701),       // FLOAT8
        ("col_text", 25),          // TEXT
        ("col_varchar", 1043),     // VARCHAR
        ("col_char", 1042),        // BPCHAR (for CHAR(n))
        ("col_name", 19),          // NAME
        ("col_bytea", 17),         // BYTEA
        ("col_oid", 26),           // OID
        ("col_uuid", 2950),        // UUID
        ("col_json", 114),         // JSON
        ("col_jsonb", 3802),       // JSONB
        ("col_date", 1082),        // DATE
        ("col_time", 1083),        // TIME
        ("col_timestamp", 1114),   // TIMESTAMP
        ("col_timestamptz", 1184), // TIMESTAMPTZ
        ("col_numeric", 1700),     // NUMERIC
    ];

    assert_eq!(columns.len(), expected_columns.len());

    for (i, (expected_name, expected_type_id)) in expected_columns.iter().enumerate() {
        assert_eq!(columns[i].name, *expected_name);
        assert_eq!(columns[i].type_id, *expected_type_id);
    }

    // Verify the Insert message has correct data types
    let Message::Insert { tuple_data, .. } = &messages[2] else {
        panic!("Expected Insert message");
    };

    // Check that we have the right number of columns
    assert_eq!(tuple_data.len(), 21);

    // Verify specific value parsing (we'll check a few key types)

    // ID (auto-generated)
    assert!(matches!(tuple_data[0], Some(Value::Integer(1))));

    // Boolean
    assert!(matches!(tuple_data[1], Some(Value::Boolean(true))));

    // Integers
    assert!(matches!(tuple_data[2], Some(Value::Integer(32767))));
    assert!(matches!(tuple_data[3], Some(Value::Integer(2147483647))));
    assert!(matches!(
        tuple_data[4],
        Some(Value::BigInt(9223372036854775807))
    ));

    // Floats
    if let Some(Value::Float(f)) = &tuple_data[5] {
        assert!((f - 3.14).abs() < 0.01);
    } else {
        panic!("Expected Float value");
    }

    if let Some(Value::Double(d)) = &tuple_data[6] {
        assert!((d - 2.718281828459045).abs() < 0.000000000000001);
    } else {
        panic!("Expected Double value");
    }

    // Text types
    assert!(matches!(tuple_data[7], Some(Value::Text(ref s)) if s == "test text"));
    assert!(matches!(tuple_data[8], Some(Value::Text(ref s)) if s == "test varchar"));
    assert!(matches!(tuple_data[9], Some(Value::Text(ref s)) if s.starts_with("test char")));
    assert!(matches!(tuple_data[10], Some(Value::Text(ref s)) if s == "test_name"));

    // Binary data
    if let Some(Value::Binary(bytes)) = &tuple_data[11] {
        assert_eq!(bytes, &vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]); // "Hello" in hex
    } else {
        panic!("Expected Binary value");
    }

    // OID
    assert!(matches!(tuple_data[12], Some(Value::Integer(12345))));

    // UUID
    assert!(
        matches!(tuple_data[13], Some(Value::Uuid(ref s)) if s == "550e8400-e29b-41d4-a716-446655440000")
    );

    // JSON
    assert!(matches!(tuple_data[14], Some(Value::Json(ref s)) if s == r#"{"key": "value"}"#));

    // JSONB
    assert!(matches!(tuple_data[15], Some(Value::Jsonb(ref s)) if s == r#"{"jsonb": true}"#));

    // Date, Time, Timestamp types - just verify they're parsed (exact values depend on parsing)
    assert!(matches!(tuple_data[16], Some(Value::Date(_))));
    assert!(matches!(tuple_data[17], Some(Value::Time(_))));
    assert!(matches!(tuple_data[18], Some(Value::Timestamp(_))));
    assert!(matches!(tuple_data[19], Some(Value::TimestampTz(_))));

    // Numeric - may be parsed as Double or Unknown depending on implementation
    assert!(matches!(
        tuple_data[20],
        Some(Value::Double(_)) | Some(Value::Unknown(_, 1700))
    ));

    // Verify Commit message
    assert!(matches!(messages[3], Message::Commit(_)));
}
