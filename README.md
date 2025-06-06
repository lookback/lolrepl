# lolrepl

A PostgreSQL logical replication client library.

This crate provides functionality to connect to PostgreSQL and stream logical
replication data (Write-Ahead Log messages) through the replication protocol.
It supports authentication, replication slot management, and parsing of WAL messages.

## Features

- Establishes replication connections to PostgreSQL
- Handles authentication (cleartext and MD5)
- Parses logical replication WAL messages
- Supports various PostgreSQL data types
- Provides error handling for replication operations

## Prerequisites

Before using this library, you need to set up PostgreSQL for logical replication:

1. Configure PostgreSQL with logical replication enabled in `postgresql.conf`:
   ```text
   wal_level = logical
   max_replication_slots = 10
   max_wal_senders = 10
   ```

2. Create a replication user in PostgreSQL:
   ```sql
   CREATE USER replication_user WITH REPLICATION LOGIN PASSWORD 'password';
   ```

3. Create a publication for the tables you want to replicate:
   ```sql
   CREATE PUBLICATION my_publication FOR TABLE users, orders;
   ```

4. Create a replication slot:
   ```sql
   SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
   ```

## Basic Usage

### Simple Replication Consumer

```rust
use std::net::TcpStream;
use lolrepl::{Connection, Subscriber, Message};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to PostgreSQL
    let tcp_stream = TcpStream::connect("localhost:5432")?;

    // Establish replication connection
    let connection = Connection::new(
        tcp_stream,
        "replication_user",
        "password",
        "mydb"
    )?;

    // Create subscriber
    let mut subscriber = Subscriber::new(
        connection,
        "my_slot",
        "my_publication"
    )?;

    // Stream replication messages
    loop {
        match subscriber.next()? {
            Message::Begin(lsn) => {
                println!("Transaction started at LSN: {}", lsn);
            }
            Message::Relation { id, namespace, name, .. } => {
                println!("Relation definition: {}.{} (ID: {})", namespace, name, id);
            }
            Message::Insert { relation_id, tuple_data } => {
                if let Some(relation) = subscriber.relation_info(relation_id) {
                    println!("INSERT into {}.{}", relation.namespace, relation.name);
                    for (i, value) in tuple_data.iter().enumerate() {
                        if let Some(col) = relation.columns.get(i) {
                            println!("  {}: {:?}", col.name, value);
                        }
                    }
                }
            }
            Message::Update { relation_id, old_tuple_data, new_tuple_data } => {
                if let Some(relation) = subscriber.relation_info(relation_id) {
                    println!("UPDATE on {}.{}", relation.namespace, relation.name);
                    println!("  New values:");
                    for (i, value) in new_tuple_data.iter().enumerate() {
                        if let Some(col) = relation.columns.get(i) {
                            println!("    {}: {:?}", col.name, value);
                        }
                    }
                }
            }
            Message::Delete { relation_id, old_tuple_data } => {
                if let Some(relation) = subscriber.relation_info(relation_id) {
                    println!("DELETE from {}.{}", relation.namespace, relation.name);
                    if let Some(old_data) = old_tuple_data {
                        for (i, value) in old_data.iter().enumerate() {
                            if let Some(col) = relation.columns.get(i) {
                                println!("  {}: {:?}", col.name, value);
                            }
                        }
                    }
                }
            }
            Message::Commit(lsn) => {
                println!("Transaction committed at LSN: {}", lsn);
            }
            Message::Unknown(msg_type) => {
                println!("Unknown message type: {}", msg_type);
            }
        }
    }
}
```

### Processing Specific Value Types

```rust
use lolrepl::{Value, Message};

fn process_insert_message(tuple_data: &[Option<Value>]) {
    for (i, value_opt) in tuple_data.iter().enumerate() {
        match value_opt {
            Some(Value::Text(s)) => println!("Column {}: Text = '{}'", i, s),
            Some(Value::Integer(n)) => println!("Column {}: Integer = {}", i, n),
            Some(Value::BigInt(n)) => println!("Column {}: BigInt = {}", i, n),
            Some(Value::Boolean(b)) => println!("Column {}: Boolean = {}", i, b),
            Some(Value::Timestamp(ts)) => println!("Column {}: Timestamp = {}", i, ts),
            Some(Value::Uuid(uuid)) => println!("Column {}: UUID = {}", i, uuid),
            Some(Value::Json(json)) => println!("Column {}: JSON = {}", i, json),
            Some(Value::Binary(data)) => println!("Column {}: Binary ({} bytes)", i, data.len()),
            Some(other) => println!("Column {}: {:?}", i, other),
            None => println!("Column {}: NULL", i),
        }
    }
}
```

### Error Handling

```rust
use lolrepl::{Connection, Subscriber, Error};
use std::net::TcpStream;

fn setup_replication() -> Result<(), Error> {
    let tcp_stream = TcpStream::connect("localhost:5432")
        .map_err(|e| Error::Io(e))?;

    let connection = Connection::new(
        tcp_stream,
        "replication_user",
        "password",
        "mydb"
    )?;

    let mut subscriber = Subscriber::new(
        connection,
        "my_slot",
        "my_publication"
    )?;

    loop {
        match subscriber.next() {
            Ok(message) => {
                // Process message
                println!("Received: {:?}", message);
            }
            Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::TimedOut => {
                // Timeout is normal, continue
                continue;
            }
            Err(e) => {
                eprintln!("Replication error: {}", e);
                return Err(e);
            }
        }
    }
}
```
