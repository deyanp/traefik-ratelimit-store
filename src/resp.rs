//! RESP2 framing.
//!
//! Only the subset a rate-limit client exercises is implemented: inbound commands are
//! always arrays of bulk strings, and outbound replies are simple strings, errors, or
//! arrays of bulk strings.

use crate::errors::ProtocolError;

/// Refuses a length prefix large enough to be a memory-exhaustion attempt. The longest
/// legitimate argument is the script source, which is a couple of kilobytes.
const MAX_ELEMENT_LENGTH: i64 = 1 << 20;

/// The most arguments any accepted command carries, with headroom.
const MAX_ARGUMENT_COUNT: i64 = 64;

/// What the store sends back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// `+OK`-style status line.
    Simple(&'static str),
    /// `-ERR …` error line. The caller treats these as data, not as a broken connection.
    Error(String),
    /// `$…` length-prefixed string.
    Bulk(String),
    /// `*…` array.
    Array(Vec<Reply>),
}

/// The outcome of attempting to read one command from a connection buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseOutcome {
    /// A whole command was read, consuming `consumed` bytes from the front of the buffer.
    Complete { args: Vec<Vec<u8>>, consumed: usize },
    /// The buffer holds a prefix of a command; read more and try again.
    Incomplete,
}

/// Finds the end of a CRLF-terminated line starting at `from`.
///
/// Returns the index of the `\r`, or `None` when the terminator has not arrived yet.
fn find_line_end(buffer: &[u8], from: usize) -> Option<usize> {
    let mut index = from;
    while index + 1 < buffer.len() {
        if buffer[index] == b'\r' && buffer[index + 1] == b'\n' {
            return Some(index);
        }
        index += 1;
    }
    None
}

/// Reads the decimal number following a type byte at `from`.
///
/// Returns the value and the index just past the line's CRLF.
fn parse_length(buffer: &[u8], from: usize) -> Result<Option<(i64, usize)>, ProtocolError> {
    let Some(line_end) = find_line_end(buffer, from) else {
        return Ok(None);
    };

    let digits =
        std::str::from_utf8(&buffer[from..line_end]).map_err(|_| ProtocolError::MalformedLength)?;
    let value: i64 = digits
        .trim()
        .parse()
        .map_err(|_| ProtocolError::MalformedLength)?;

    Ok(Some((value, line_end + 2)))
}

/// Reads one bulk string beginning at `from`, returning it and the index just past it.
fn parse_bulk_argument(
    buffer: &[u8],
    from: usize,
) -> Result<Option<(Vec<u8>, usize)>, ProtocolError> {
    if from >= buffer.len() {
        return Ok(None);
    }
    if buffer[from] != b'$' {
        return Err(ProtocolError::NonBulkArgument(buffer[from]));
    }

    let Some((length, after_length)) = parse_length(buffer, from + 1)? else {
        return Ok(None);
    };
    if !(0..=MAX_ELEMENT_LENGTH).contains(&length) {
        return Err(ProtocolError::LengthTooLarge(length));
    }

    let length = length as usize;
    let end = after_length + length;
    if buffer.len() < end + 2 {
        return Ok(None);
    }
    if &buffer[end..end + 2] != b"\r\n" {
        return Err(ProtocolError::MissingTerminator);
    }

    Ok(Some((buffer[after_length..end].to_vec(), end + 2)))
}

/// Attempts to read one command from the front of `buffer`.
pub fn parse_command(buffer: &[u8]) -> Result<ParseOutcome, ProtocolError> {
    if buffer.is_empty() {
        return Ok(ParseOutcome::Incomplete);
    }
    if buffer[0] != b'*' {
        return Err(ProtocolError::UnexpectedType(buffer[0]));
    }

    let Some((count, mut cursor)) = parse_length(buffer, 1)? else {
        return Ok(ParseOutcome::Incomplete);
    };
    if !(0..=MAX_ARGUMENT_COUNT).contains(&count) {
        return Err(ProtocolError::LengthTooLarge(count));
    }

    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let Some((argument, next)) = parse_bulk_argument(buffer, cursor)? else {
            return Ok(ParseOutcome::Incomplete);
        };
        args.push(argument);
        cursor = next;
    }

    Ok(ParseOutcome::Complete {
        args,
        consumed: cursor,
    })
}

/// Appends the wire form of `reply` to `out`.
pub fn encode_reply(reply: &Reply, out: &mut Vec<u8>) {
    match reply {
        Reply::Simple(text) => {
            out.push(b'+');
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Error(text) => {
            out.push(b'-');
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Bulk(text) => {
            out.push(b'$');
            out.extend_from_slice(text.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Array(items) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_reply(item, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(reply: &Reply) -> String {
        let mut out = Vec::new();
        encode_reply(reply, &mut out);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn parses_a_whole_command() {
        let wire = b"*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n";

        let outcome = parse_command(wire).unwrap();

        assert_eq!(
            outcome,
            ParseOutcome::Complete {
                args: vec![b"PING".to_vec(), b"hi".to_vec()],
                consumed: wire.len(),
            }
        );
    }

    #[test]
    fn reports_incomplete_until_the_last_byte_arrives() {
        let wire = b"*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n";

        for split in 0..wire.len() {
            assert_eq!(
                parse_command(&wire[..split]).unwrap(),
                ParseOutcome::Incomplete
            );
        }
    }

    #[test]
    fn leaves_trailing_bytes_for_the_next_command() {
        let wire = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";

        let ParseOutcome::Complete { consumed, .. } = parse_command(wire).unwrap() else {
            panic!("expected a complete command");
        };

        assert_eq!(consumed, wire.len() / 2);
    }

    #[test]
    fn rejects_a_request_that_is_not_an_array() {
        assert_eq!(
            parse_command(b"+OK\r\n"),
            Err(ProtocolError::UnexpectedType(b'+'))
        );
    }

    #[test]
    fn rejects_an_oversized_length_prefix() {
        assert_eq!(
            parse_command(b"*1\r\n$99999999\r\n"),
            Err(ProtocolError::LengthTooLarge(99_999_999))
        );
    }

    #[test]
    fn encodes_the_script_reply_shape() {
        let reply = Reply::Array(vec![
            Reply::Bulk("true".to_string()),
            Reply::Bulk("0".to_string()),
            Reply::Bulk("9".to_string()),
        ]);

        assert_eq!(
            encoded(&reply),
            "*3\r\n$4\r\ntrue\r\n$1\r\n0\r\n$1\r\n9\r\n"
        );
    }

    #[test]
    fn encodes_status_and_error_lines() {
        assert_eq!(encoded(&Reply::Simple("PONG")), "+PONG\r\n");
        assert_eq!(
            encoded(&Reply::Error("NOSCRIPT No matching script.".to_string())),
            "-NOSCRIPT No matching script.\r\n"
        );
    }
}
