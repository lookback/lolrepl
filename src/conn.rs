use crate::Error;
use md5;
use std::io::{Read, Write};

// PostgreSQL message representation
pub(crate) struct PgMessage {
    pub message_type: u8,
    pub data: Vec<u8>,
}

/// A PostgreSQL replication connection that handles the replication protocol.
///
/// This struct manages a connection to PostgreSQL specifically for logical replication.
/// It handles authentication, startup messages, and provides methods for reading and
/// writing PostgreSQL protocol messages in the context of replication.
pub struct Connection<T: Read + Write> {
    stream: T,
}

impl<T: Read + Write> Connection<T> {
    /// Create a new replication connection with an existing stream.
    ///
    /// This method establishes a replication connection by sending startup messages,
    /// handling authentication, and preparing the connection for replication operations.
    ///
    /// # Arguments
    ///
    /// * `stream` - A stream that implements Read + Write (typically a TCP connection)
    /// * `user` - The PostgreSQL username for authentication
    /// * `password` - The password for authentication
    /// * `database` - The database name to connect to
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the `Connection` on success, or an `Error` on failure.
    pub fn new(stream: T, user: &str, password: &str, database: &str) -> Result<Self, Error> {
        let mut connection = Connection { stream };

        connection.send_startup_message(user, database)?;
        connection.handle_authentication(user, password)?;
        connection.process_startup_messages()?;

        Ok(connection)
    }

    // Send startup message with replication flag
    fn send_startup_message(&mut self, user: &str, database: &str) -> Result<(), Error> {
        // Protocol version (3.0)
        let protocol_version: i32 = 196608; // 3.0 in format: 3 << 16 | 0

        // Build the startup packet
        let mut packet = Vec::new();
        packet.extend_from_slice(&protocol_version.to_be_bytes());

        // Add parameters
        self.write_string_param(&mut packet, "user", user)?;
        self.write_string_param(&mut packet, "database", database)?;
        self.write_string_param(&mut packet, "replication", "database")?;

        // Add terminating null byte
        packet.push(0);

        let mut full_message = Vec::new();

        // Length includes itself (4 bytes)
        let length = (packet.len() + 4) as i32;
        full_message.extend_from_slice(&length.to_be_bytes());
        full_message.extend_from_slice(&packet);

        self.stream.write_all(&full_message)?;

        Ok(())
    }

    // Helper to write string parameters
    fn write_string_param(
        &self,
        packet: &mut Vec<u8>,
        name: &str,
        value: &str,
    ) -> Result<(), Error> {
        packet.extend_from_slice(name.as_bytes());
        packet.push(0); // null terminator
        packet.extend_from_slice(value.as_bytes());
        packet.push(0); // null terminator
        Ok(())
    }

    // Handle authentication flow
    fn handle_authentication(&mut self, user: &str, password: &str) -> Result<(), Error> {
        let message = self.read_message(false)?;

        match message.message_type {
            b'R' => {
                // Authentication request
                if message.data.len() < 4 {
                    return Err(Error::InvalidAuthRequest);
                }

                let auth_type = i32::from_be_bytes([
                    message.data[0],
                    message.data[1],
                    message.data[2],
                    message.data[3],
                ]);

                match auth_type {
                    0 => {
                        // AuthenticationOk - no password needed
                    }
                    3 => {
                        // ClearTextPassword
                        self.send_password_message(password)?;

                        // Wait for AuthenticationOk
                        self.handle_authentication(user, password)?;
                    }
                    5 => {
                        // MD5Password
                        if message.data.len() < 8 {
                            return Err(Error::InvalidMd5AuthRequest);
                        }

                        let salt = &message.data[4..8];

                        // Use the stored username from the connection
                        let md5_password = self.compute_md5_password(password, user, salt)?;
                        self.send_password_message(&md5_password)?;

                        // Wait for AuthenticationOk
                        self.handle_authentication(user, password)?;
                    }
                    _ => {
                        return Err(Error::Authentication(format!(
                            "Unsupported authentication method: {}",
                            auth_type
                        )));
                    }
                }
            }
            b'E' => {
                // ErrorResponse
                let error_message = self.parse_error_message(&message.data)?;
                return Err(Error::Authentication(error_message));
            }
            _ => {
                // Unexpected message
                return Err(Error::ReplicationProtocolViolation(format!(
                    "Unexpected message type during authentication: {}",
                    message.message_type as char
                )));
            }
        }

        Ok(())
    }

    // Send password message
    fn send_password_message(&mut self, password: &str) -> Result<(), Error> {
        let mut password_data = Vec::new();
        password_data.extend_from_slice(password.as_bytes());
        password_data.push(0); // null terminator

        self.write_message(b'p', &password_data, false)?;
        Ok(())
    }

    // Compute MD5 password hash
    fn compute_md5_password(
        &self,
        password: &str,
        username: &str,
        salt: &[u8],
    ) -> Result<String, Error> {
        // PostgreSQL MD5 authentication format: md5 + hex(md5(md5(password + user) + salt))

        // Step 1: md5(password + user)
        let mut hasher = md5::Context::new();
        hasher.consume(password.as_bytes());
        hasher.consume(username.as_bytes());
        let inner_hash = hasher.compute();

        // Step 2: md5(inner_hash + salt)
        let mut hasher = md5::Context::new();
        hasher.consume(format!("{:x}", inner_hash).as_bytes());
        hasher.consume(salt);
        let outer_hash = hasher.compute();

        // Step 3: "md5" + hex(outer_hash)
        let result = format!("md5{:x}", outer_hash);

        Ok(result)
    }

    // Parse error message
    fn parse_error_message(&self, data: &[u8]) -> Result<String, Error> {
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

    // Process startup messages until ready
    fn process_startup_messages(&mut self) -> Result<(), Error> {
        let mut ready_for_query = false;

        while !ready_for_query {
            let message = self.read_message(false)?;

            match message.message_type {
                b'S' => {
                    // ParameterStatus message
                    // Process parameter=value pairs if needed
                }
                b'K' => {
                    // BackendKeyData message
                    if message.data.len() >= 8 {
                        let _process_id = i32::from_be_bytes([
                            message.data[0],
                            message.data[1],
                            message.data[2],
                            message.data[3],
                        ]);
                        let _secret_key = i32::from_be_bytes([
                            message.data[4],
                            message.data[5],
                            message.data[6],
                            message.data[7],
                        ]);
                    }
                }
                b'Z' => {
                    // ReadyForQuery message
                    ready_for_query = true;
                }
                b'E' => {
                    // ErrorResponse
                    let error_message = self.parse_error_message(&message.data)?;
                    return Err(Error::ServerStartupFailure(error_message));
                }
                b'N' => {
                    // NoticeResponse
                    let notice = self.parse_error_message(&message.data)?;
                    eprintln!("Notice: {}", notice);
                }
                _ => {
                    eprintln!(
                        "Unhandled message type during startup: {}",
                        message.message_type as char
                    );
                }
            }
        }

        Ok(())
    }

    /// Read a PostgreSQL protocol message from the connection.
    ///
    /// This method reads a complete PostgreSQL message from the underlying stream.
    /// It handles both regular messages and COPY data messages used in replication.
    ///
    /// # Arguments
    ///
    /// * `copy_data` - Whether to expect COPY data format (used during replication streaming)
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the `PgMessage` on success, or an `Error` on failure.
    pub(crate) fn read_message(&mut self, copy_data: bool) -> Result<PgMessage, Error> {
        // Read 5 byte header: Type (1) + Length (4)
        let mut header = [0u8; 5];
        self.stream.read_exact(&mut header)?;

        let message_type = header[0];
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let data_length = length - 4;

        // Read the message data
        let mut data = vec![0u8; data_length];
        self.stream.read_exact(&mut data)?;

        // If not in copy mode or message isn't CopyData, return as is
        if !copy_data || message_type != b'd' || data.is_empty() {
            return Ok(PgMessage { message_type, data });
        }

        // In copy mode, extract real message from CopyData
        Ok(PgMessage {
            message_type: data[0],
            data: data[1..].to_vec(),
        })
    }

    /// Send a PostgreSQL protocol message through the connection.
    ///
    /// This method formats and sends a PostgreSQL protocol message with the specified
    /// message type and data payload.
    ///
    /// # Arguments
    ///
    /// * `message_type` - The PostgreSQL message type byte
    /// * `data` - The message payload data
    /// * `copy_data` - Whether this is COPY data format (used during replication streaming)
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating success or an `Error` on failure.
    pub(crate) fn write_message(
        &mut self,
        message_type: u8,
        data: &[u8],
        copy_data: bool,
    ) -> Result<(), Error> {
        let mut length = (data.len() + 4) as i32;

        if copy_data {
            length += 1;
        }

        let mut buffer = Vec::with_capacity(length as usize);

        if copy_data {
            // COPY mode - wrap in CopyData message
            buffer.push(b'd'); // CopyData message type
        } else {
            // Normal mode
            buffer.push(message_type);
        }

        buffer.extend_from_slice(&length.to_be_bytes());

        if copy_data {
            buffer.push(message_type);
        }

        buffer.extend_from_slice(data);

        self.stream.write_all(&buffer)?;
        Ok(())
    }
}
