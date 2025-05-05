use crate::Error;
use crate::conn::Connection;
use crate::value::{Value, parse_binary_value, parse_text_value};

use jiff;
use std::collections::HashMap;
use std::io::{Read, Write};

// Helper functions for reading binary data directly from slices - make these private
fn read_u64_from_slice(data: &[u8]) -> Result<u64, Error> {
    if data.len() < 8 {
        return Err(Error::UnexpectedEndOfData("u64"));
    }
    let value = u64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);
    Ok(value)
}

fn read_i64_from_slice(data: &[u8]) -> Result<i64, Error> {
    if data.len() < 8 {
        return Err(Error::UnexpectedEndOfData("i64"));
    }
    let value = i64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);
    Ok(value)
}

// Parse PostgreSQL error/notice message format - make this private
fn parse_pg_error_message(data: &[u8]) -> Result<String, Error> {
    let mut error_message = String::new();
    let mut i = 0;

    while i < data.len() {
        let field_type = data[i];
        i += 1;

        if field_type == 0 {
            break; // End of message
        }

        let mut field_value = Vec::new();
        while i < data.len() && data[i] != 0 {
            field_value.push(data[i]);
            i += 1;
        }
        i += 1; // Skip null terminator

        if let Ok(value) = std::str::from_utf8(&field_value) {
            match field_type {
                b'M' => error_message = value.to_string(), // Message
                b'S' => error_message = format!("{}: {}", value, error_message), // Severity
                _ => {}                                    // Ignore other fields
            }
        }
    }

    Ok(error_message)
}

/// Represents a Write-Ahead Log (WAL) message from PostgreSQL logical replication.
///
/// These messages correspond to different types of changes that occur in the database
/// and are sent as part of the logical replication stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Begin transaction with the given Log Sequence Number (LSN).
    Begin(u64), // LSN
    /// Relation (table) definition message.
    Relation {
        /// The OID of the relation.
        id: u32,
        /// The namespace (schema) name of the relation.
        namespace: String,
        /// The name of the relation.
        name: String,
        /// The replica identity setting for the relation.
        replica_identity: u8,
        /// The column definitions for the relation.
        columns: Vec<Column>,
    },
    /// Insert operation on a table.
    Insert {
        /// The OID of the relation (table) where the insert occurred.
        relation_id: u32,
        /// The data values for the new tuple being inserted.
        tuple_data: Vec<Option<Value>>,
    },
    /// Update operation on a table.
    Update {
        /// The OID of the relation (table) where the update occurred.
        relation_id: u32,
        /// The old tuple data before the update (if available).
        old_tuple_data: Option<Vec<Option<Value>>>,
        /// The new tuple data after the update.
        new_tuple_data: Vec<Option<Value>>,
    },
    /// Delete operation on a table.
    Delete {
        /// The OID of the relation (table) where the delete occurred.
        relation_id: u32,
        /// The old tuple data that was deleted (if available).
        old_tuple_data: Option<Vec<Option<Value>>>,
    },
    /// Commit transaction with the given Log Sequence Number (LSN).
    Commit(u64), // LSN
    /// Unknown message type with the raw message type byte.
    Unknown(u8),
}

/// Information about a PostgreSQL relation (table) used in replication.
///
/// This struct holds metadata about a table that is being replicated,
/// including its schema, name, columns, and replica identity setting.
pub struct RelationInfo {
    /// The namespace (schema) name of the relation.
    pub namespace: String,
    /// The name of the relation.
    pub name: String,
    /// The column definitions for the relation.
    pub columns: Vec<Column>,
    /// The replica identity setting for the relation.
    pub replica_identity: u8,
}

/// Represents a column in a PostgreSQL relation.
///
/// Contains metadata about a table column including its name, type information,
/// and any flags that describe its attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The name of the column.
    pub name: String,
    /// The PostgreSQL type OID for this column.
    pub type_id: u32,
    /// The type modifier for this column (used for precision, length, etc.).
    pub type_modifier: i32,
    /// Flags that describe column attributes (e.g., part of primary key).
    pub flags: u8, // Add flags field to track column attributes
}

/// A PostgreSQL logical replication subscriber.
///
/// This struct manages a replication connection and handles the streaming of
/// logical replication data from PostgreSQL. It maintains relation metadata
/// and provides methods to consume WAL messages.
pub struct Subscriber<T: Read + Write> {
    connection: Connection<T>,
    slot_name: String,
    publication_name: String,
    relation_cache: HashMap<u32, RelationInfo>,
    last_received_lsn: u64,
    last_status_update: std::time::Instant,
}

impl<T: Read + Write> Subscriber<T> {
    /// Create a new subscriber with an existing replication connection.
    ///
    /// This method initializes a new `Subscriber` and starts the replication
    /// process using the specified replication slot and publication.
    ///
    /// # Arguments
    ///
    /// * `connection` - An established `Connection`
    /// * `slot_name` - The name of the replication slot to use
    /// * `publication_name` - The name of the publication to subscribe to
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the `Subscriber` on success, or an `Error` on failure.
    pub fn new(
        connection: Connection<T>,
        slot_name: &str,
        publication_name: &str,
    ) -> Result<Self, Error> {
        let mut subscriber = Subscriber {
            connection,
            slot_name: slot_name.to_string(),
            publication_name: publication_name.to_string(),
            relation_cache: HashMap::new(),
            last_received_lsn: 0,
            last_status_update: std::time::Instant::now(),
        };

        subscriber.start_replication()?;

        Ok(subscriber)
    }

    // Start the replication process
    fn start_replication(&mut self) -> Result<(), Error> {
        // Send START_REPLICATION command
        let start_replication_command = format!(
            "START_REPLICATION SLOT {} LOGICAL 0/0 (proto_version '1', publication_names '{}')",
            self.slot_name, self.publication_name
        );

        // Format as a Query message
        let mut query_data = Vec::new();

        // Command
        query_data.extend_from_slice(start_replication_command.as_bytes());
        query_data.push(0); // null terminator

        self.connection.write_message(b'Q', &query_data, false)?;

        // Process the server's response
        let copy_mode_started;

        loop {
            let message = self.connection.read_message(false)?;

            match message.message_type {
                b'W' => {
                    // CopyBothResponse - replication starts
                    copy_mode_started = true;
                    break; // Exit the message loop, we're now in streaming mode
                }
                b'E' => {
                    // ErrorResponse
                    let error_message = parse_pg_error_message(&message.data)?;
                    return Err(Error::ReplicationCommandFailed(error_message));
                }
                _ => {
                    eprintln!("Unexpected message type: {}", message.message_type as char);
                }
            }
        }

        if !copy_mode_started {
            return Err(Error::ReplicationCopyModeNotStarted);
        }

        Ok(())
    }

    /// Get information about a relation by its ID.
    ///
    /// Returns the cached relation information for the given relation ID,
    /// or `None` if the relation is not known.
    ///
    /// # Arguments
    ///
    /// * `relation_id` - The OID of the relation to look up
    ///
    /// # Returns
    ///
    /// Returns an `Option` containing a reference to the `RelationInfo` if found.
    pub fn relation_info(&self, relation_id: u32) -> Option<&RelationInfo> {
        self.relation_cache.get(&relation_id)
    }

    /// Get the next WAL message from the replication stream.
    ///
    /// This method blocks until a new WAL message is available and returns it.
    /// It also handles periodic status updates to the server to maintain the
    /// replication connection.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the next `Message` on success, or an `Error` on failure.
    pub fn next(&mut self) -> Result<Message, Error> {
        loop {
            // Send standby status update every 10 seconds
            if self.last_status_update.elapsed().as_secs() >= 10 {
                self.send_standby_status_update()?;
                self.last_status_update = std::time::Instant::now();
            }

            // Read the next message - using copy_data=true since we're in replication mode
            let message = match self.connection.read_message(true) {
                Ok(msg) => msg,
                Err(e) => {
                    // Handle timeouts gracefully - need to adjust for our custom Error type
                    if let Error::Io(io_err) = &e {
                        if io_err.kind() == std::io::ErrorKind::WouldBlock
                            || io_err.kind() == std::io::ErrorKind::TimedOut
                        {
                            // Just a timeout, try again
                            continue;
                        }
                    }
                    return Err(e); // Other error, propagate
                }
            };

            match message.message_type {
                b'k' => {
                    // Primary keepalive message
                    if message.data.len() >= 17 {
                        // 8 (LSN) + 8 (timestamp) + 1 (reply flag)
                        let wal_end = read_u64_from_slice(&message.data[0..8])?;

                        // Update our LSN tracking
                        if wal_end > self.last_received_lsn {
                            self.last_received_lsn = wal_end;
                        }

                        // Check if server wants a reply
                        let reply_required = message.data[16] != 0;

                        if reply_required {
                            self.send_standby_status_update()?;
                            self.last_status_update = std::time::Instant::now();
                        }
                    }

                    continue; // This was just a keepalive, continue to get a real message
                }
                b'w' => {
                    // WAL data message
                    if message.data.len() > 24 {
                        // Extract WAL start position and current end position
                        let _wal_start = read_u64_from_slice(&message.data[0..8])?;
                        let wal_end = read_u64_from_slice(&message.data[8..16])?;
                        let _server_time = read_i64_from_slice(&message.data[16..24])?;

                        // Update our position tracking
                        if wal_end > self.last_received_lsn {
                            self.last_received_lsn = wal_end;
                        }

                        // Parse the actual WAL message payload
                        let mut wal_data = &message.data[24..];

                        // Parse the WAL data into a message
                        let wal_message = self.parse_wal_data(&mut wal_data)?;

                        // Store relation info in the cache if this is a Relation message
                        if let Message::Relation {
                            id,
                            namespace,
                            name,
                            replica_identity,
                            columns,
                        } = &wal_message
                        {
                            self.relation_cache.insert(
                                *id,
                                RelationInfo {
                                    namespace: namespace.clone(),
                                    name: name.clone(),
                                    columns: columns.clone(),
                                    replica_identity: *replica_identity,
                                },
                            );
                        }

                        // Return the message to the caller
                        return Ok(wal_message);
                    }

                    continue;
                }
                b'E' => {
                    // Error message
                    let error_message = parse_pg_error_message(&message.data)?;
                    eprintln!("Error from server: {}", error_message);
                    continue; // This was just an error message, continue to get a real message
                }
                _ => {
                    eprintln!("Unhandled message type: {}", message.message_type as char);
                    continue; // Unknown message, continue to get a real message
                }
            }
        }
    }

    // Send standby status update to the server - this should be private since it's an implementation detail
    fn send_standby_status_update(&mut self) -> Result<(), Error> {
        // Build the standby status message - Format according to PostgreSQL protocol:
        // - Int64 - write position (LSN)
        // - Int64 - flush position (LSN)
        // - Int64 - apply position (LSN)
        // - Int64 - client timestamp
        // - Byte1 - reply requested flag (0/1)

        let mut message_data = Vec::new();

        // Current WAL position (LSN) that we've received and written
        message_data.extend_from_slice(&self.last_received_lsn.to_be_bytes());

        // Current WAL position (LSN) that we've flushed to disk - use same value for simplicity
        message_data.extend_from_slice(&self.last_received_lsn.to_be_bytes());

        // Current WAL position (LSN) that we've applied - use same value for simplicity
        message_data.extend_from_slice(&self.last_received_lsn.to_be_bytes());

        // Current system clock time
        let now = jiff::Zoned::now();
        let pg_time = now.timestamp().as_microsecond() - 946684800000000; // Microseconds since 2000-01-01
        message_data.extend_from_slice(&pg_time.to_be_bytes());

        // Reply requested flag (0 = no reply needed)
        message_data.push(0);

        // Send as CopyData message with type 'r' since we're in COPY mode during replication
        self.connection.write_message(b'r', &message_data, true)?;

        Ok(())
    }

    // Parse WAL data - make private as it's an implementation detail
    fn parse_wal_data(&self, data: &mut &[u8]) -> Result<Message, Error> {
        if data.is_empty() {
            return Err(Error::EmptyWalData);
        }

        // The first byte indicates the message type
        let message_type = data[0];
        *data = &data[1..]; // Consume the message type byte

        match message_type {
            b'B' => {
                // Begin message
                let lsn = self.read_lsn(data)?;
                Ok(Message::Begin(lsn))
            }
            b'C' => {
                // Commit message
                let lsn = self.read_lsn(data)?;
                Ok(Message::Commit(lsn))
            }
            b'I' => {
                // Insert message
                let relation_id = self.read_u32(data)?;

                // Debug print the tuple format
                if !data.is_empty() {
                    // Check for the 'N' flag that indicates tuple data
                    if data[0] == b'N' {
                        // Skip the 'N' flag byte
                        *data = &data[1..];
                    }
                }

                // Now read the tuple data
                let tuple_data = self.read_tuple_data(data, relation_id)?;

                // Insert message handling
                // ...

                Ok(Message::Insert {
                    relation_id,
                    tuple_data,
                })
            }
            b'U' => {
                // Update message
                let relation_id = self.read_u32(data)?;

                // Read old tuple data if present (depends on replica identity setting)
                let has_old_tuple = self.read_u8(data)? == b'O';

                let old_tuple_data = if has_old_tuple {
                    Some(self.read_tuple_data(data, relation_id)?)
                } else {
                    None
                };

                // Read new tuple data
                let new_tuple_data = self.read_tuple_data(data, relation_id)?;

                Ok(Message::Update {
                    relation_id,
                    old_tuple_data,
                    new_tuple_data,
                })
            }
            b'D' => {
                // Delete message
                let relation_id = self.read_u32(data)?;

                // Read old tuple data if present (depends on replica identity setting)
                let has_old_tuple = self.read_u8(data)? == b'O';

                let old_tuple_data = if has_old_tuple {
                    Some(self.read_tuple_data(data, relation_id)?)
                } else {
                    None
                };

                Ok(Message::Delete {
                    relation_id,
                    old_tuple_data,
                })
            }
            b'R' => {
                // Relation message (table schema)
                let id = self.read_u32(data)?;

                let namespace = self.read_string(data)?;

                let name = self.read_string(data)?;

                let replica_identity = self.read_u8(data)?;

                // Read column information
                let column_count = self.read_u16(data)?;

                let mut columns = Vec::with_capacity(column_count as usize);

                // PostgreSQL logical replication column format:
                // For each column:
                // - 1 byte: column flags (can indicate key column)
                // - string: column name
                // - 4 bytes: type OID
                // - 4 bytes: type modifier
                for _ in 0..column_count {
                    // Read column flags first
                    let flags = self.read_u8(data)?;

                    // Then read the column name
                    let name = self.read_string(data)?;

                    let type_id = self.read_u32(data)?;

                    let type_modifier = self.read_i32(data)?;

                    columns.push(Column {
                        name,
                        type_id,
                        type_modifier,
                        flags,
                    });
                }

                Ok(Message::Relation {
                    id,
                    namespace,
                    name,
                    replica_identity,
                    columns,
                })
            }
            _ => {
                eprintln!("Unknown message type: {}", message_type);
                Ok(Message::Unknown(message_type))
            }
        }
    }

    // Helper methods for parsing binary data - make all these private

    fn read_u8(&self, data: &mut &[u8]) -> Result<u8, Error> {
        if data.is_empty() {
            return Err(Error::UnexpectedEndOfData("u8"));
        }
        let value = data[0];
        *data = &data[1..];
        Ok(value)
    }

    fn read_u16(&self, data: &mut &[u8]) -> Result<u16, Error> {
        if data.len() < 2 {
            return Err(Error::UnexpectedEndOfData("u16"));
        }
        let value = u16::from_be_bytes([data[0], data[1]]);
        *data = &data[2..];
        Ok(value)
    }

    fn read_u32(&self, data: &mut &[u8]) -> Result<u32, Error> {
        if data.len() < 4 {
            return Err(Error::UnexpectedEndOfData("u32"));
        }
        let value = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        *data = &data[4..];
        Ok(value)
    }

    fn read_i32(&self, data: &mut &[u8]) -> Result<i32, Error> {
        if data.len() < 4 {
            return Err(Error::UnexpectedEndOfData("i32"));
        }
        let value = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        *data = &data[4..];
        Ok(value)
    }

    fn read_lsn(&self, data: &mut &[u8]) -> Result<u64, Error> {
        if data.len() < 8 {
            return Err(Error::UnexpectedEndOfData("LSN"));
        }
        let value = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        *data = &data[8..];
        Ok(value)
    }

    fn read_string(&self, data: &mut &[u8]) -> Result<String, Error> {
        let mut len = 0;
        while len < data.len() && data[len] != 0 {
            len += 1;
        }

        if len >= data.len() {
            return Err(Error::UnterminatedString);
        }

        // Convert bytes to string
        let result = match std::str::from_utf8(&data[..len]) {
            Ok(s) => s.to_string(),
            Err(_) => {
                // Try to sanitize by replacing invalid UTF-8 sequences
                String::from_utf8_lossy(&data[..len]).into_owned()
            }
        };

        // Consume the string plus null terminator
        *data = &data[len + 1..];

        Ok(result)
    }

    fn read_tuple_data(
        &self,
        data: &mut &[u8],
        relation_id: u32,
    ) -> Result<Vec<Option<Value>>, Error> {
        // Check for the 'N' flag that indicates tuple data in the newer pgoutput format
        if !data.is_empty() && data[0] == b'N' {
            *data = &data[1..];
        }

        // Try to get relation info from cache
        let relation_info = self.relation_cache.get(&relation_id);

        // Get the column count from the tuple data, not the relation cache
        // pgoutput format includes a column count in the tuple data
        let column_count = self.read_u16(data)?;

        let mut tuple_data = Vec::with_capacity(column_count as usize);

        // For each column in the tuple
        for i in 0..column_count {
            // Get column info from relation cache if available
            let type_id = if let Some(rel) = relation_info {
                if (i as usize) < rel.columns.len() {
                    let column = &rel.columns[i as usize];
                    column.type_id
                } else {
                    // Fallback if column index is out of bounds
                    25 // Default to text type
                }
            } else {
                // Fallback if relation not in cache
                25
            };

            // Read the data format flag
            let format_byte = self.read_u8(data)?;

            match format_byte {
                b't' => {
                    // Text format - value follows with a 4-byte length
                    let len = self.read_i32(data)?;

                    if len < 0 {
                        // NULL value in text format
                        tuple_data.push(None);
                    } else {
                        if data.len() < len as usize {
                            return Err(Error::UnexpectedEndOfData("column value"));
                        }

                        // Try to decode as UTF-8, fall back to lossy if needed
                        let text_slice = &data[..len as usize];
                        *data = &data[len as usize..];

                        // Parse the text value directly from the slice
                        let text_str = std::str::from_utf8(text_slice)?;

                        // Use the helper function to parse text value
                        let value = parse_text_value(text_str, type_id)?;
                        tuple_data.push(Some(value));
                    }
                }
                b'b' => {
                    // Binary format - value follows with a 4-byte length
                    let len = self.read_i32(data)?;

                    if len < 0 {
                        // NULL value in binary format
                        tuple_data.push(None);
                    } else {
                        if data.len() < len as usize {
                            return Err(Error::UnexpectedEndOfData("binary column value"));
                        }

                        // Extract binary data
                        let binary_data = &data[..len as usize];

                        // Parse binary data - pass the binary data, type_id, and length
                        let value = match parse_binary_value(binary_data, type_id, len) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("Warning: Failed to parse binary value: {}", e);
                                Value::Binary(binary_data.to_vec())
                            }
                        };
                        tuple_data.push(Some(value));

                        // Advance data pointer
                        *data = &data[len as usize..];
                    }
                }
                b'n' => {
                    // NULL value
                    tuple_data.push(None);
                }
                b'u' => {
                    // Unchanged value (treated as null for INSERT)
                    tuple_data.push(None);
                }
                _ => {
                    // Try to continue rather than failing
                    tuple_data.push(None);
                }
            }
        }

        Ok(tuple_data)
    }
}
