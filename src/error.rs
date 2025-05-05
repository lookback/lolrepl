use std::fmt;
use std::io;
use std::string::FromUtf8Error;

/// Error types that can occur during PostgreSQL replication operations.
///
/// This enum encompasses all possible errors that can happen when establishing
/// connections, authenticating, parsing data, and handling replication streams.
#[derive(Debug)]
pub enum Error {
    /// IO related errors from underlying network operations.
    Io(io::Error),

    /// Data reading errors
    /// Occurs when expecting more data but reaching end of stream unexpectedly.
    UnexpectedEndOfData(&'static str),
    /// Empty WAL data received when data was expected.
    EmptyWalData,
    /// Empty COPY data received when data was expected.
    EmptyCopyData,
    /// String data is not properly null-terminated.
    UnterminatedString,

    /// Authentication errors
    /// Invalid or malformed authentication request from server.
    InvalidAuthRequest,
    /// Invalid or malformed MD5 authentication request from server.
    InvalidMd5AuthRequest,
    /// General authentication failure with descriptive message.
    Authentication(String),

    /// Replication errors
    /// A replication command sent to the server failed.
    ReplicationCommandFailed(String),
    /// Failed to enter COPY mode needed for replication streaming.
    ReplicationCopyModeNotStarted,
    /// Server violated the replication protocol in some way.
    ReplicationProtocolViolation(String),
    /// Replication stream timed out waiting for data.
    ReplicationStreamTimedOut,

    /// Startup errors
    /// Server startup process failed with error message.
    ServerStartupFailure(String),
    /// Backend key data received during startup has invalid format.
    BackendKeyDataInvalid,
    /// Parameter status message received during startup has invalid format.
    ParameterStatusInvalid,

    /// Data parsing errors
    /// UTF-8 decoding error when parsing string data.
    Utf8(std::str::Utf8Error),
    /// Error decoding hexadecimal data.
    HexDecode(hex::FromHexError),

    /// Value parsing errors
    /// Error parsing integer values from text or binary data.
    ParseInt(std::num::ParseIntError),
    /// Error parsing floating-point values from text or binary data.
    ParseFloat(std::num::ParseFloatError),
    /// Error parsing date/time values using the jiff library.
    ParseDateTime(jiff::Error),
    /// General value parsing failure with descriptive message.
    ParseValue(String), // For general value parsing failures
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "IO error: {}", err),
            Error::UnexpectedEndOfData(context) => {
                write!(f, "Unexpected end of data while reading {}", context)
            }
            Error::EmptyWalData => write!(f, "Empty WAL data"),
            Error::EmptyCopyData => write!(f, "Empty COPY data"),
            Error::UnterminatedString => write!(f, "Unterminated string"),
            Error::InvalidAuthRequest => write!(f, "Invalid authentication request"),
            Error::InvalidMd5AuthRequest => write!(f, "Invalid MD5 auth request"),
            Error::Authentication(msg) => write!(f, "Authentication error: {}", msg),

            // Updated replication errors
            Error::ReplicationCommandFailed(msg) => {
                write!(f, "Replication command failed: {}", msg)
            }
            Error::ReplicationCopyModeNotStarted => {
                write!(f, "Failed to enter copy mode for replication")
            }
            Error::ReplicationProtocolViolation(msg) => {
                write!(f, "Replication protocol violation: {}", msg)
            }
            Error::ReplicationStreamTimedOut => {
                write!(f, "Replication stream timed out waiting for data")
            }

            // Updated startup errors
            Error::ServerStartupFailure(msg) => write!(f, "Server startup failure: {}", msg),
            Error::BackendKeyDataInvalid => write!(f, "Invalid backend key data format"),
            Error::ParameterStatusInvalid => write!(f, "Invalid parameter status format"),

            Error::Utf8(err) => write!(f, "UTF-8 error: {}", err),
            Error::HexDecode(err) => write!(f, "Hex decode error: {}", err),
            Error::ParseInt(err) => write!(f, "Integer parse error: {}", err),
            Error::ParseFloat(err) => write!(f, "Float parse error: {}", err),
            Error::ParseDateTime(err) => write!(f, "DateTime parsing error: {}", err),
            Error::ParseValue(msg) => write!(f, "Value parse error: {}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            Error::Utf8(err) => Some(err),
            Error::HexDecode(err) => Some(err),
            Error::ParseInt(err) => Some(err),
            Error::ParseFloat(err) => Some(err),
            Error::ParseDateTime(err) => Some(err),
            _ => None,
        }
    }
}

// Implement From conversions for common error types
impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Error::Utf8(err)
    }
}

impl From<hex::FromHexError> for Error {
    fn from(err: hex::FromHexError) -> Self {
        Error::HexDecode(err)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::ParseInt(err)
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        Error::ParseFloat(err)
    }
}

impl From<jiff::Error> for Error {
    fn from(err: jiff::Error) -> Self {
        Error::ParseDateTime(err)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error::Utf8(err.utf8_error())
    }
}
