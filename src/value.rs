use crate::Error;
use jiff::{Zoned, civil};

// PostgreSQL type OIDs for common types
pub const PG_TYPE_BOOL: u32 = 16;
pub const PG_TYPE_BYTEA: u32 = 17;
pub const PG_TYPE_CHAR: u32 = 18;
pub const PG_TYPE_NAME: u32 = 19;
pub const PG_TYPE_INT8: u32 = 20;
pub const PG_TYPE_INT2: u32 = 21;
pub const PG_TYPE_INT4: u32 = 23;
pub const PG_TYPE_TEXT: u32 = 25;
pub const PG_TYPE_OID: u32 = 26;
pub const PG_TYPE_JSON: u32 = 114;
pub const PG_TYPE_FLOAT4: u32 = 700;
pub const PG_TYPE_FLOAT8: u32 = 701;
pub const PG_TYPE_BPCHAR: u32 = 1042;
pub const PG_TYPE_VARCHAR: u32 = 1043;
pub const PG_TYPE_DATE: u32 = 1082;
pub const PG_TYPE_TIME: u32 = 1083;
pub const PG_TYPE_TIMESTAMP: u32 = 1114;
pub const PG_TYPE_TIMESTAMPTZ: u32 = 1184;
pub const PG_TYPE_NUMERIC: u32 = 1700;
pub const PG_TYPE_UUID: u32 = 2950;
pub const PG_TYPE_JSONB: u32 = 3802;

/// Represents various PostgreSQL data types and their values.
///
/// This enum provides a unified representation for different PostgreSQL data types
/// that can be encountered during logical replication. It handles type conversions
/// and provides display formatting for the values.
#[derive(Debug, Clone)]
pub enum Value {
    /// Text or character data (VARCHAR, TEXT, CHAR, etc.).
    Text(String),
    /// 32-bit integer values (INTEGER, INT4).
    Integer(i32),
    /// 64-bit integer values (BIGINT, INT8).
    BigInt(i64),
    /// 32-bit floating-point values (REAL, FLOAT4).
    Float(f32),
    /// 64-bit floating-point values (DOUBLE PRECISION, FLOAT8).
    Double(f64),
    /// Boolean values (true/false).
    Boolean(bool),
    /// Date values without time information.
    Date(civil::Date),
    /// Time values without date information.
    Time(civil::Time),
    /// Timestamp values without timezone information.
    Timestamp(civil::DateTime),
    /// Timestamp values with timezone information.
    TimestampTz(Zoned),
    /// UUID values stored as string representation.
    Uuid(String), // Using String representation for simplicity
    /// JSON data stored as string.
    Json(String), // JSON as string for simplicity
    /// JSONB data stored as string.
    Jsonb(String), // JSONB as string for simplicity
    /// Binary data (BYTEA).
    Binary(Vec<u8>),
    /// NULL values.
    Null,
    /// Unknown or unsupported type with raw bytes and type OID.
    Unknown(Vec<u8>, u32), // Raw bytes and type OID for unknown types
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Text(s) => write!(f, "{}", s),
            Value::Integer(i) => write!(f, "{}", i),
            Value::BigInt(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Double(d) => write!(f, "{}", d),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Date(d) => write!(f, "{}", d),
            Value::Time(t) => write!(f, "{}", t),
            Value::Timestamp(ts) => write!(f, "{}", ts),
            Value::TimestampTz(ts) => write!(f, "{}", ts),
            Value::Uuid(u) => write!(f, "{}", u),
            Value::Json(j) => write!(f, "{}", j),
            Value::Jsonb(j) => write!(f, "{}", j),
            Value::Binary(b) => write!(f, "<binary data: {} bytes>", b.len()),
            Value::Null => write!(f, "NULL"),
            Value::Unknown(_, oid) => write!(f, "<unknown type: {}>", oid),
        }
    }
}

// Helper function to parse text value into appropriate Value based on type_id
pub fn parse_text_value(text: &str, type_id: u32) -> Result<Value, Error> {
    match type_id {
        PG_TYPE_BOOL => {
            // PostgreSQL bool is 't' or 'f'
            if text == "t" {
                Ok(Value::Boolean(true))
            } else if text == "f" {
                Ok(Value::Boolean(false))
            } else {
                Err(Error::ParseValue(format!(
                    "Invalid boolean value: {}",
                    text
                )))
            }
        }
        PG_TYPE_INT2 | PG_TYPE_INT4 => match text.parse::<i32>() {
            Ok(num) => Ok(Value::Integer(num)),
            Err(e) => Err(Error::ParseInt(e)),
        },
        PG_TYPE_INT8 => match text.parse::<i64>() {
            Ok(num) => Ok(Value::BigInt(num)),
            Err(e) => Err(Error::ParseInt(e)),
        },
        PG_TYPE_FLOAT4 => match text.parse::<f32>() {
            Ok(num) => Ok(Value::Float(num)),
            Err(e) => Err(Error::ParseFloat(e)),
        },
        PG_TYPE_FLOAT8 => match text.parse::<f64>() {
            Ok(num) => Ok(Value::Double(num)),
            Err(e) => Err(Error::ParseFloat(e)),
        },
        PG_TYPE_UUID => Ok(Value::Uuid(text.to_string())),
        PG_TYPE_JSON => Ok(Value::Json(text.to_string())),
        PG_TYPE_JSONB => Ok(Value::Jsonb(text.to_string())),
        PG_TYPE_TEXT | PG_TYPE_VARCHAR | PG_TYPE_CHAR | PG_TYPE_NAME | PG_TYPE_BPCHAR => {
            Ok(Value::Text(text.to_string()))
        }
        PG_TYPE_BYTEA => {
            // Handle bytea hex format \x followed by hex digits
            if text.starts_with("\\x") {
                match hex::decode(&text[2..]) {
                    Ok(bytes) => Ok(Value::Binary(bytes)),
                    Err(e) => Err(Error::HexDecode(e)),
                }
            } else {
                Err(Error::ParseValue(format!("Invalid bytea format: {}", text)))
            }
        }
        PG_TYPE_DATE => match civil::Date::strptime("%Y-%m-%d", text) {
            Ok(date) => Ok(Value::Date(date)),
            Err(e) => Err(Error::ParseDateTime(e)),
        },
        PG_TYPE_TIMESTAMP => match civil::DateTime::strptime("%Y-%m-%d %H:%M:%S%.f", text) {
            Ok(dt) => Ok(Value::Timestamp(dt)),
            Err(e) => Err(Error::ParseDateTime(e)),
        },
        PG_TYPE_TIMESTAMPTZ => match parse_timestamptz(text) {
            Ok(dt) => Ok(Value::TimestampTz(dt)),
            Err(e) => Err(e),
        },
        PG_TYPE_TIME => {
            // Time can have various formats, try to parse
            match civil::Time::strptime("%H:%M:%S%.f", text) {
                Ok(time) => Ok(Value::Time(time)),
                Err(e) => Err(Error::ParseDateTime(e)),
            }
        }
        PG_TYPE_NUMERIC => {
            // Try to parse as f64 for simplicity, though this may lose precision for NUMERIC
            match text.parse::<f64>() {
                Ok(num) => Ok(Value::Double(num)),
                Err(e) => Err(Error::ParseFloat(e)),
            }
        }
        PG_TYPE_OID => match text.parse::<u32>() {
            Ok(oid) => Ok(Value::Integer(oid as i32)),
            Err(e) => Err(Error::ParseInt(e)),
        },
        _ => Err(Error::ParseValue(format!("Unknown type_id: {}", type_id))),
    }
}

// Helper function to parse binary value into appropriate Value based on type_id
pub fn parse_binary_value(binary_data: &[u8], type_id: u32, len: i32) -> Result<Value, Error> {
    match type_id {
        PG_TYPE_BOOL if len == 1 => Ok(Value::Boolean(binary_data[0] != 0)),
        PG_TYPE_INT2 if len == 2 => {
            let value = i16::from_be_bytes([binary_data[0], binary_data[1]]);
            Ok(Value::Integer(value as i32))
        }
        PG_TYPE_INT4 if len == 4 => {
            let value = i32::from_be_bytes([
                binary_data[0],
                binary_data[1],
                binary_data[2],
                binary_data[3],
            ]);
            Ok(Value::Integer(value))
        }
        PG_TYPE_INT8 if len == 8 => {
            let value = i64::from_be_bytes([
                binary_data[0],
                binary_data[1],
                binary_data[2],
                binary_data[3],
                binary_data[4],
                binary_data[5],
                binary_data[6],
                binary_data[7],
            ]);
            Ok(Value::BigInt(value))
        }
        PG_TYPE_FLOAT4 if len == 4 => {
            let value = f32::from_be_bytes([
                binary_data[0],
                binary_data[1],
                binary_data[2],
                binary_data[3],
            ]);
            Ok(Value::Float(value))
        }
        PG_TYPE_FLOAT8 if len == 8 => {
            let value = f64::from_be_bytes([
                binary_data[0],
                binary_data[1],
                binary_data[2],
                binary_data[3],
                binary_data[4],
                binary_data[5],
                binary_data[6],
                binary_data[7],
            ]);
            Ok(Value::Double(value))
        }
        PG_TYPE_BYTEA => {
            let mut bytes = Vec::with_capacity(len as usize);
            bytes.extend_from_slice(binary_data);
            Ok(Value::Binary(bytes))
        }
        PG_TYPE_TEXT | PG_TYPE_VARCHAR | PG_TYPE_CHAR | PG_TYPE_NAME | PG_TYPE_BPCHAR => {
            // Convert to UTF-8 string
            match std::str::from_utf8(binary_data) {
                Ok(s) => Ok(Value::Text(s.to_string())),
                Err(_) => {
                    // If not valid UTF-8, store as binary
                    let mut bytes = Vec::with_capacity(len as usize);
                    bytes.extend_from_slice(binary_data);
                    Ok(Value::Binary(bytes))
                }
            }
        }
        PG_TYPE_OID if len == 4 => {
            let value = u32::from_be_bytes([
                binary_data[0],
                binary_data[1],
                binary_data[2],
                binary_data[3],
            ]);
            Ok(Value::Integer(value as i32))
        }
        PG_TYPE_UUID if len == 16 => {
            // Format UUID from 16 bytes
            let uuid_str = format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                binary_data[0],
                binary_data[1],
                binary_data[2],
                binary_data[3],
                binary_data[4],
                binary_data[5],
                binary_data[6],
                binary_data[7],
                binary_data[8],
                binary_data[9],
                binary_data[10],
                binary_data[11],
                binary_data[12],
                binary_data[13],
                binary_data[14],
                binary_data[15]
            );
            Ok(Value::Uuid(uuid_str))
        }
        PG_TYPE_JSON | PG_TYPE_JSONB => {
            // Parse JSON from binary representation
            match std::str::from_utf8(binary_data) {
                Ok(json_str) => {
                    if type_id == PG_TYPE_JSON {
                        Ok(Value::Json(json_str.to_string()))
                    } else {
                        Ok(Value::Jsonb(json_str.to_string()))
                    }
                }
                Err(_) => {
                    // If not valid UTF-8, store as binary
                    let mut bytes = Vec::with_capacity(len as usize);
                    bytes.extend_from_slice(binary_data);
                    Ok(Value::Binary(bytes))
                }
            }
        }
        PG_TYPE_DATE if len == 4 => {
            // PostgreSQL date binary format: days since 2000-01-01
            let days_since_epoch = i32::from_be_bytes([
                binary_data[0],
                binary_data[1],
                binary_data[2],
                binary_data[3],
            ]);

            // PostgreSQL epoch is 2000-01-01
            let pg_epoch = civil::Date::constant(2000, 1, 1);
            match pg_epoch.checked_add(jiff::Span::new().days(days_since_epoch as i64)) {
                Ok(date) => Ok(Value::Date(date)),
                Err(_) => {
                    // If date calculation fails, store as raw binary
                    Ok(Value::Unknown(binary_data.to_vec(), type_id))
                }
            }
        }
        PG_TYPE_TIMESTAMP if len == 8 => {
            // PostgreSQL timestamp binary format: microseconds since 2000-01-01 00:00:00
            let microsecs = i64::from_be_bytes([
                binary_data[0],
                binary_data[1],
                binary_data[2],
                binary_data[3],
                binary_data[4],
                binary_data[5],
                binary_data[6],
                binary_data[7],
            ]);

            // PostgreSQL epoch is 2000-01-01
            let pg_epoch = civil::Date::constant(2000, 1, 1);
            let pg_epoch_dt = pg_epoch.at(0, 0, 0, 0);

            match pg_epoch_dt.checked_add(jiff::Span::new().microseconds(microsecs)) {
                Ok(dt) => Ok(Value::Timestamp(dt)),
                Err(_) => {
                    // If timestamp calculation fails, store as raw binary
                    Ok(Value::Unknown(binary_data.to_vec(), type_id))
                }
            }
        }
        PG_TYPE_TIMESTAMPTZ if len == 8 => {
            // PostgreSQL timestamptz binary format: microseconds since 2000-01-01 00:00:00 UTC
            let microsecs = i64::from_be_bytes([
                binary_data[0],
                binary_data[1],
                binary_data[2],
                binary_data[3],
                binary_data[4],
                binary_data[5],
                binary_data[6],
                binary_data[7],
            ]);

            // PostgreSQL epoch is 2000-01-01
            let pg_epoch = civil::Date::constant(2000, 1, 1);
            let pg_epoch_dt = pg_epoch.at(0, 0, 0, 0);

            match pg_epoch_dt.in_tz("UTC") {
                Ok(pg_epoch_zoned) => {
                    match pg_epoch_zoned.checked_add(jiff::Span::new().microseconds(microsecs)) {
                        Ok(dt) => Ok(Value::TimestampTz(dt)),
                        Err(_) => {
                            // If timestamp calculation fails, store as raw binary
                            Ok(Value::Unknown(binary_data.to_vec(), type_id))
                        }
                    }
                }
                Err(_) => {
                    // If timezone conversion fails, store as raw binary
                    Ok(Value::Unknown(binary_data.to_vec(), type_id))
                }
            }
        }
        PG_TYPE_TIME if len == 8 => {
            // PostgreSQL time binary format: microseconds since 00:00:00
            let microsecs = i64::from_be_bytes([
                binary_data[0],
                binary_data[1],
                binary_data[2],
                binary_data[3],
                binary_data[4],
                binary_data[5],
                binary_data[6],
                binary_data[7],
            ]);

            // Convert to civil::Time
            let secs = (microsecs / 1_000_000) as u32;

            let hours = (secs / 3600) as i8;
            let minutes = ((secs % 3600) / 60) as i8;
            let seconds = (secs % 60) as i8;
            let nsecs = ((microsecs % 1_000_000) * 1_000) as i32;

            match civil::Time::new(hours, minutes, seconds, nsecs) {
                Ok(time) => Ok(Value::Time(time)),
                Err(_) => Ok(Value::Unknown(binary_data.to_vec(), type_id)),
            }
        }
        PG_TYPE_NUMERIC => {
            // For NUMERIC, we just store the binary data since proper parsing is complex
            // A full implementation would require understanding the binary format
            Ok(Value::Unknown(binary_data.to_vec(), type_id))
        }
        _ => {
            // For unknown or unhandled types, store the raw bytes and type OID
            Ok(Value::Unknown(binary_data.to_vec(), type_id))
        }
    }
}

// Helper function to parse timestamptz with timezone normalization
fn parse_timestamptz(text: &str) -> Result<Zoned, Error> {
    // PostgreSQL outputs timestamptz in different formats depending on server timezone:
    // - "+HH" (whole hours like +01)
    // - "+HH:MM" (with minutes like +05:30 for Indian timezone)
    // The %z format expects "+HHMM" (4 digits, no colon), so normalize both formats

    // Early return for text too short to contain timezone
    if text.len() < 3 {
        return match Zoned::strptime("%Y-%m-%d %H:%M:%S%.f%z", text) {
            Ok(dt) => Ok(dt),
            Err(e) => Err(Error::ParseDateTime(e)),
        };
    }

    // Early return if no timezone part found
    let Some(tz_start) = text.rfind(&['+', '-'][..]) else {
        return match Zoned::strptime("%Y-%m-%d %H:%M:%S%.f%z", text) {
            Ok(dt) => Ok(dt),
            Err(e) => Err(Error::ParseDateTime(e)),
        };
    };

    let main_part = &text[..tz_start];
    let tz_part = &text[tz_start..];
    let mut normalized_buf = [0u8; 256];

    // Copy main part to buffer
    let main_bytes = main_part.as_bytes();
    normalized_buf[..main_bytes.len()].copy_from_slice(main_bytes);
    let mut buf_pos = main_bytes.len();

    // Normalize timezone part based on format
    if tz_part.len() == 6 && tz_part.chars().nth(3) == Some(':') {
        // "+HH:MM" format -> "+HHMM"
        let tz_bytes = tz_part.as_bytes();
        normalized_buf[buf_pos] = tz_bytes[0]; // sign
        normalized_buf[buf_pos + 1] = tz_bytes[1]; // H
        normalized_buf[buf_pos + 2] = tz_bytes[2]; // H
        normalized_buf[buf_pos + 3] = tz_bytes[4]; // M (skip colon)
        normalized_buf[buf_pos + 4] = tz_bytes[5]; // M
        buf_pos += 5;
    } else if tz_part.len() == 3 {
        // "+HH" format -> "+HH00"
        let tz_bytes = tz_part.as_bytes();
        normalized_buf[buf_pos] = tz_bytes[0]; // sign
        normalized_buf[buf_pos + 1] = tz_bytes[1]; // H
        normalized_buf[buf_pos + 2] = tz_bytes[2]; // H
        normalized_buf[buf_pos + 3] = b'0'; // M
        normalized_buf[buf_pos + 4] = b'0'; // M
        buf_pos += 5;
    } else {
        // Unknown format, copy as-is
        let tz_bytes = tz_part.as_bytes();
        normalized_buf[buf_pos..buf_pos + tz_bytes.len()].copy_from_slice(tz_bytes);
        buf_pos += tz_bytes.len();
    }

    // Use buffer directly as &str for strptime
    let normalized_str = std::str::from_utf8(&normalized_buf[..buf_pos])
        .map_err(|_| Error::ParseValue("Invalid UTF-8 in normalized timestamp".to_string()))?;

    match Zoned::strptime("%Y-%m-%d %H:%M:%S%.f%z", normalized_str) {
        Ok(dt) => Ok(dt),
        Err(e) => Err(Error::ParseDateTime(e)),
    }
}

// Manually implement PartialEq and Eq for Value to compare f32/f64 with tolerance
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::BigInt(a), Value::BigInt(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => (a - b).abs() < f32::EPSILON,
            (Value::Double(a), Value::Double(b)) => (a - b).abs() < f64::EPSILON,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Time(a), Value::Time(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::TimestampTz(a), Value::TimestampTz(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            (Value::Jsonb(a), Value::Jsonb(b)) => a == b,
            (Value::Binary(a), Value::Binary(b)) => a == b,
            (Value::Null, Value::Null) => true,
            (Value::Unknown(a, oid_a), Value::Unknown(b, oid_b)) => a == b && oid_a == oid_b,
            _ => false,
        }
    }
}

impl Eq for Value {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamptz_whole_hour_positive() {
        // Test +HH format (whole hours)
        let result = parse_timestamptz("2023-12-25 14:30:45.123456+01");
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.to_string(), "2023-12-25T14:30:45.123456+01:00[+01:00]");
    }

    #[test]
    fn test_parse_timestamptz_whole_hour_negative() {
        // Test -HH format (negative whole hours)
        let result = parse_timestamptz("2023-12-25 14:30:45.123456-05");
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.to_string(), "2023-12-25T14:30:45.123456-05:00[-05:00]");
    }

    #[test]
    fn test_parse_timestamptz_indian_timezone() {
        // Test +HH:MM format (Indian Standard Time)
        let result = parse_timestamptz("2023-11-12 19:30:00.000000+05:30");
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.to_string(), "2023-11-12T19:30:00+05:30[+05:30]");
    }

    #[test]
    fn test_parse_timestamptz_negative_half_hour() {
        // Test -HH:MM format (negative half hour offset)
        let result = parse_timestamptz("2023-11-12 19:30:00.000000-03:30");
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.to_string(), "2023-11-12T19:30:00-03:30[-03:30]");
    }

    #[test]
    fn test_parse_timestamptz_without_microseconds() {
        // Test format without microseconds
        let result = parse_timestamptz("2023-12-25 14:30:45+01");
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.to_string(), "2023-12-25T14:30:45+01:00[+01:00]");
    }

    #[test]
    fn test_parse_timestamptz_utc() {
        // Test UTC timezone (+00)
        let result = parse_timestamptz("2023-12-25 14:30:45.123456+00");
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.to_string(), "2023-12-25T14:30:45.123456+00:00[UTC]");
    }

    #[test]
    fn test_parse_timestamptz_various_minute_offsets() {
        // Test various minute offsets
        let test_cases = vec![
            ("2023-01-01 12:00:00+04:30", "+04:30"),
            ("2023-01-01 12:00:00+09:30", "+09:30"),
            ("2023-01-01 12:00:00-02:15", "-02:15"),
            ("2023-01-01 12:00:00+14:00", "+14:00"),
        ];

        for (input, expected_tz) in test_cases {
            let result = parse_timestamptz(input);
            assert!(result.is_ok(), "Failed to parse: {}", input);
            let dt = result.unwrap();
            assert!(
                dt.to_string().contains(expected_tz),
                "Expected timezone {} in {}",
                expected_tz,
                dt.to_string()
            );
        }
    }

    #[test]
    fn test_parse_timestamptz_short_text() {
        // Test text too short to contain timezone
        let result = parse_timestamptz("ab");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamptz_invalid_format() {
        // Test invalid timestamp format
        let result = parse_timestamptz("not-a-timestamp+01");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamptz_no_timezone() {
        // Test timestamp without timezone marker
        let result = parse_timestamptz("2023-12-25 14:30:45.123456");
        assert!(result.is_err()); // Should fail because %z requires timezone
    }

    #[test]
    fn test_parse_timestamptz_unknown_timezone_format() {
        // Test unknown timezone format (should copy as-is and likely fail parsing)
        let result = parse_timestamptz("2023-12-25 14:30:45.123456+1234");
        // This should actually work because +1234 is valid for %z
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_text_value_timestamptz_integration() {
        // Test integration with parse_text_value function
        let test_cases = vec![
            "2023-12-25 14:30:45.123456+01",
            "2023-11-12 19:30:00.000000+05:30",
            "2023-12-25 14:30:45.123456-05",
        ];

        for input in test_cases {
            let result = parse_text_value(input, PG_TYPE_TIMESTAMPTZ);
            assert!(
                result.is_ok(),
                "Failed to parse via parse_text_value: {}",
                input
            );
            match result.unwrap() {
                Value::TimestampTz(_) => {} // Expected
                other => panic!("Expected TimestampTz, got: {:?}", other),
            }
        }
    }
}
