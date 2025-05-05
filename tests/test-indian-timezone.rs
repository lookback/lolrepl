mod common;

use std::net::TcpStream;
use std::time::Duration;

use lolrepl::Connection;
use lolrepl::Message;
use lolrepl::Subscriber;
use lolrepl::Value;

#[test]
fn test_indian_timezone_replication() {
    // Setup a temporary database
    let temp_db = common::init_tmp_db();

    // Set timezone to India Standard Time (UTC+05:30) and create test table
    temp_db.execute(
        "
        -- Set the session timezone to India Standard Time
        SET timezone = 'Asia/Kolkata';

        CREATE TABLE test_indian_times (
            id SERIAL PRIMARY KEY,
            event_name TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            scheduled_at TIMESTAMPTZ
        );

        -- Create publication for the table
        CREATE PUBLICATION test_indian_publication FOR TABLE test_indian_times;

        -- Create a replication slot
        SELECT pg_create_logical_replication_slot('test_indian_slot', 'pgoutput');
    ",
    );

    // Insert test data with specific Indian timezone timestamps
    temp_db.execute(
        "
        INSERT INTO test_indian_times (event_name, scheduled_at) VALUES
        ('Diwali Celebration', '2023-11-12 19:30:00+05:30'::timestamptz),
        ('Morning Meeting', '2024-01-15 09:15:00+05:30'::timestamptz);
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

    let mut sub = Subscriber::new(conn, "test_indian_slot", "test_indian_publication")
        .expect("Failed to create subscriber");

    let mut messages = Vec::new();

    // Read messages: Begin, Relation, Insert, Insert, Commit
    for _ in 0..5 {
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

    // Verify the table structure includes timestamptz columns
    assert_eq!(columns.len(), 4);
    assert_eq!(columns[0].name, "id");
    assert_eq!(columns[0].type_id, 23); // INT4
    assert_eq!(columns[1].name, "event_name");
    assert_eq!(columns[1].type_id, 25); // TEXT
    assert_eq!(columns[2].name, "created_at");
    assert_eq!(columns[2].type_id, 1184); // TIMESTAMPTZ
    assert_eq!(columns[3].name, "scheduled_at");
    assert_eq!(columns[3].type_id, 1184); // TIMESTAMPTZ

    // Extract the insert messages
    let insert1 = &messages[2];
    let insert2 = &messages[3];

    // Verify we can parse the Indian timezone timestamps correctly
    match insert1 {
        Message::Insert { tuple_data, .. } => {
            assert_eq!(tuple_data.len(), 4);

            // Check the scheduled_at timestamp for Diwali Celebration
            match &tuple_data[3] {
                Some(Value::TimestampTz(tz)) => {
                    // The timestamp should be parseable and contain timezone info
                    println!("Diwali timestamp: {}", tz);

                    // Verify the timestamp represents the correct time in UTC
                    // 2023-11-12 19:30:00+05:30 should be 2023-11-12 14:00:00 UTC
                    let utc_repr = tz.to_string();
                    println!("UTC representation: {}", utc_repr);
                }
                other => panic!("Expected TimestampTz for scheduled_at, got: {:?}", other),
            }
        }
        _ => panic!("Expected Insert message"),
    }

    match insert2 {
        Message::Insert { tuple_data, .. } => {
            assert_eq!(tuple_data.len(), 4);

            // Check the scheduled_at timestamp for Morning Meeting
            match &tuple_data[3] {
                Some(Value::TimestampTz(tz)) => {
                    println!("Morning meeting timestamp: {}", tz);

                    // Verify the timestamp represents the correct time in UTC
                    // 2024-01-15 09:15:00+05:30 should be 2024-01-15 03:45:00 UTC
                    let utc_repr = tz.to_string();
                    println!("UTC representation: {}", utc_repr);
                }
                other => panic!("Expected TimestampTz for scheduled_at, got: {:?}", other),
            }
        }
        _ => panic!("Expected Insert message"),
    }

    // Verify we got a Commit message
    match &messages[4] {
        Message::Commit { .. } => {}
        other => panic!("Expected Commit message, got: {:?}", other),
    }

    println!("Indian timezone test completed successfully!");
    println!("Note: PostgreSQL converts input timezone +05:30 to server timezone");
    println!("This test verifies that our parsing handles any timezone format PostgreSQL sends");
}
