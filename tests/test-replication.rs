mod common;

use std::net::TcpStream;
use std::time::Duration;

use lolrepl::Column;
use lolrepl::Connection;
use lolrepl::Message;
use lolrepl::Subscriber;
use lolrepl::Value;

#[test]
fn test_replication() {
    // Setup a temporary database
    let temp_db = common::init_tmp_db();

    // Create test tables and set up replication
    temp_db.execute(
        "
        CREATE TABLE test_items (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER
        );

        -- Create publication for the table
        CREATE PUBLICATION test_publication FOR TABLE test_items;

        -- Create a replication slot
        SELECT pg_create_logical_replication_slot('test_slot', 'pgoutput');
    ",
    );

    // Insert some test data
    temp_db.execute(
        "
        INSERT INTO test_items (name, value) VALUES ('item1', 100), ('item2', 200);
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

    let mut sub = Subscriber::new(conn, "test_slot", "test_publication")
        .expect("Failed to create subscriber");

    let mut messages = Vec::new();

    // We need to read exactly 4 more messages (Relation, Insert, Insert, Commit)
    for _ in 0..4 {
        let message = sub.next().expect("Failed to get replication message");
        messages.push(message);
    }

    let Message::Begin(lsn) = messages[0] else {
        panic!("Expected Begin message");
    };
    let Message::Relation {
        id: relation_id, ..
    } = messages[1]
    else {
        panic!("Expected Relation message");
    };

    // We expect exactly these messages in this order:
    // 1. Begin -
    // 2. Relation - describing the table
    // 3. Insert - first row
    // 4. Insert - second row
    // 5. Commit - end of transaction
    assert_eq!(
        messages,
        vec![
            Message::Begin(lsn),
            Message::Relation {
                id: relation_id,
                namespace: "public".to_string(),
                name: "test_items".to_string(),
                replica_identity: 100,
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        type_id: 23,
                        flags: 1,
                        type_modifier: -1,
                    },
                    Column {
                        name: "name".to_string(),
                        type_id: 25,
                        flags: 0,
                        type_modifier: -1,
                    },
                    Column {
                        name: "value".to_string(),
                        type_id: 23,
                        flags: 0,
                        type_modifier: -1,
                    },
                ],
            },
            Message::Insert {
                relation_id,
                tuple_data: vec![
                    Some(Value::Integer(1)),
                    Some(Value::Text("item1".to_string())),
                    Some(Value::Integer(100)),
                ],
            },
            Message::Insert {
                relation_id,
                tuple_data: vec![
                    Some(Value::Integer(2)),
                    Some(Value::Text("item2".to_string())),
                    Some(Value::Integer(200)),
                ],
            },
        ]
    );
}
