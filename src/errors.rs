use std::fmt;

/// Unclassified technical failure. Carries a message that is safe to log but never
/// reaches a rate-limit client, since the store answers only the protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TechnicalError(pub String);

impl fmt::Display for TechnicalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A request that cannot be understood as a RESP2 command.
///
/// Every variant means the connection is no longer trustworthy: the framing is lost,
/// so the caller closes rather than attempting to resynchronise.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// The first byte is not the array marker a command must start with.
    UnexpectedType(u8),
    /// A length prefix is not a decimal number.
    MalformedLength,
    /// A length prefix exceeds what a command may legitimately carry.
    LengthTooLarge(i64),
    /// An element of the command array is not a bulk string.
    NonBulkArgument(u8),
    /// A line is not terminated by CRLF where the protocol requires it.
    MissingTerminator,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedType(byte) => {
                write!(
                    f,
                    "expected a command array, found type byte {:?}",
                    *byte as char
                )
            }
            Self::MalformedLength => write!(f, "length prefix is not a number"),
            Self::LengthTooLarge(len) => {
                write!(f, "length prefix {len} exceeds the accepted maximum")
            }
            Self::NonBulkArgument(byte) => {
                write!(
                    f,
                    "command arguments must be bulk strings, found type byte {:?}",
                    *byte as char
                )
            }
            Self::MissingTerminator => write!(f, "line is not CRLF-terminated"),
        }
    }
}
