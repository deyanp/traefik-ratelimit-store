//! RESP2 framing.
//!
//! Only the subset a rate-limit client exercises is implemented: inbound commands are
//! always arrays of bulk strings, and outbound replies are simple strings, errors, or the
//! three-element array the bucket evaluation produces.
//!
//! Parsing borrows from the connection's buffer rather than copying each argument out,
//! and encoding writes straight into the connection's output buffer: a request costs no
//! heap allocation beyond the vector of argument slices.

use std::io::Write;

use crate::errors::ProtocolError;

/// Refuses a length prefix large enough to be a memory-exhaustion attempt. The longest
/// legitimate argument is the script source, which is a couple of kilobytes.
const MAX_ELEMENT_LENGTH: i64 = 1 << 20;

/// The most arguments any accepted command carries, with headroom.
const MAX_ARGUMENT_COUNT: i64 = 64;

/// What the store sends back.
#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    /// `+OK`-style status line.
    Simple(&'static str),
    /// `-ERR …` error line. The caller treats these as data, not as a broken connection.
    Error(String),
    /// The bucket evaluation's reply: a three-element array of bulk strings, in which the
    /// first is always the literal `true`, the second the wait and the third the tokens.
    /// The caller reads the first as a boolean and the second as a float; the third is
    /// returned for symmetry with the script it replaces and is not read.
    Bucket { wait: f64, tokens: f64 },
}

/// The outcome of attempting to read one command from a connection buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseOutcome<'a> {
    /// A whole command was read, consuming `consumed` bytes from the front of the buffer.
    Complete {
        args: Vec<&'a [u8]>,
        consumed: usize,
    },
    /// The buffer holds a prefix of a command. `at_least` is the buffer length below which
    /// another attempt cannot succeed, so a caller receiving one byte at a time need not
    /// re-parse the prefix on every byte.
    Incomplete { at_least: usize },
}

/// A partial result: either the value, or how long the buffer must be before retrying.
enum Step<T> {
    Done(T),
    More(usize),
}

/// Finds the end of a CRLF-terminated line starting at `from`.
///
/// Returns the index of the `\r`, or `None` when the terminator has not arrived yet.
fn find_line_end(buffer: &[u8], from: usize) -> Option<usize> {
    buffer
        .get(from..)?
        .windows(2)
        .position(|pair| pair == b"\r\n")
        .map(|offset| from + offset)
}

/// Reads the decimal number following a type byte at `from`.
///
/// Returns the value and the index just past the line's CRLF.
fn parse_length(buffer: &[u8], from: usize) -> Result<Step<(i64, usize)>, ProtocolError> {
    let Some(line_end) = find_line_end(buffer, from) else {
        // The terminator could arrive with the very next byte.
        return Ok(Step::More(buffer.len() + 1));
    };

    let digits =
        std::str::from_utf8(&buffer[from..line_end]).map_err(|_| ProtocolError::MalformedLength)?;
    let value: i64 = digits
        .trim()
        .parse()
        .map_err(|_| ProtocolError::MalformedLength)?;

    Ok(Step::Done((value, line_end + 2)))
}

/// Reads one bulk string beginning at `from`, returning it and the index just past it.
fn parse_bulk_argument(buffer: &[u8], from: usize) -> Result<Step<(&[u8], usize)>, ProtocolError> {
    if from >= buffer.len() {
        return Ok(Step::More(from + 1));
    }
    if buffer[from] != b'$' {
        return Err(ProtocolError::NonBulkArgument(buffer[from]));
    }

    let (length, after_length) = match parse_length(buffer, from + 1)? {
        Step::Done(parsed) => parsed,
        Step::More(at_least) => return Ok(Step::More(at_least)),
    };
    if !(0..=MAX_ELEMENT_LENGTH).contains(&length) {
        return Err(ProtocolError::LengthTooLarge(length));
    }

    let length = length as usize;
    let end = after_length + length;
    if buffer.len() < end + 2 {
        // The length is known, so the buffer must reach past the payload's terminator.
        return Ok(Step::More(end + 2));
    }
    if &buffer[end..end + 2] != b"\r\n" {
        return Err(ProtocolError::MissingTerminator);
    }

    Ok(Step::Done((&buffer[after_length..end], end + 2)))
}

/// Attempts to read one command from the front of `buffer`.
pub fn parse_command(buffer: &[u8]) -> Result<ParseOutcome<'_>, ProtocolError> {
    if buffer.is_empty() {
        return Ok(ParseOutcome::Incomplete { at_least: 1 });
    }
    if buffer[0] != b'*' {
        return Err(ProtocolError::UnexpectedType(buffer[0]));
    }

    let (count, mut cursor) = match parse_length(buffer, 1)? {
        Step::Done(parsed) => parsed,
        Step::More(at_least) => return Ok(ParseOutcome::Incomplete { at_least }),
    };
    if !(0..=MAX_ARGUMENT_COUNT).contains(&count) {
        return Err(ProtocolError::LengthTooLarge(count));
    }

    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (argument, next) = match parse_bulk_argument(buffer, cursor)? {
            Step::Done(parsed) => parsed,
            Step::More(at_least) => return Ok(ParseOutcome::Incomplete { at_least }),
        };
        args.push(argument);
        cursor = next;
    }

    Ok(ParseOutcome::Complete {
        args,
        consumed: cursor,
    })
}

/// Appends one bulk string holding the decimal form of `value`.
///
/// Rendered into a stack buffer first, because the length prefix has to be written before
/// the digits. Shortest round-trip representation, which the caller's float parser
/// accepts. A value too long for the stack buffer — only an absurd magnitude gets there —
/// takes the allocating path rather than being truncated.
fn encode_number_bulk(out: &mut Vec<u8>, value: f64) {
    let mut digits = [0u8; 64];
    let mut cursor = std::io::Cursor::new(&mut digits[..]);
    let rendered: &[u8] = if write!(cursor, "{value}").is_ok() {
        let length = cursor.position() as usize;
        &digits[..length]
    } else {
        let text = value.to_string();
        let _ = write!(out, "${}\r\n", text.len());
        out.extend_from_slice(text.as_bytes());
        out.extend_from_slice(b"\r\n");
        return;
    };

    let _ = write!(out, "${}\r\n", rendered.len());
    out.extend_from_slice(rendered);
    out.extend_from_slice(b"\r\n");
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
        Reply::Bucket { wait, tokens } => {
            out.extend_from_slice(b"*3\r\n$4\r\ntrue\r\n");
            encode_number_bulk(out, *wait);
            encode_number_bulk(out, *tokens);
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
                args: vec![b"PING".as_slice(), b"hi".as_slice()],
                consumed: wire.len(),
            }
        );
    }

    #[test]
    fn reports_incomplete_until_the_last_byte_arrives() {
        let wire = b"*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n";

        for split in 0..wire.len() {
            assert!(
                matches!(
                    parse_command(&wire[..split]).unwrap(),
                    ParseOutcome::Incomplete { .. }
                ),
                "split at {split}"
            );
        }
    }

    #[test]
    fn an_incomplete_prefix_says_how_much_more_it_needs() {
        // Once a bulk length is known, the parser knows exactly how long the buffer must
        // be, so a caller fed one byte at a time can skip re-parsing until then.
        let wire = b"*1\r\n$4\r\nPING\r\n";

        let ParseOutcome::Incomplete { at_least } = parse_command(&wire[..8]).unwrap() else {
            panic!("expected an incomplete command");
        };

        assert_eq!(at_least, wire.len());
        // And the hint is never a lie: the buffer at that length parses.
        assert!(matches!(
            parse_command(&wire[..at_least]).unwrap(),
            ParseOutcome::Complete { .. }
        ));
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
        let reply = Reply::Bucket {
            wait: 0.0,
            tokens: 9.0,
        };

        assert_eq!(
            encoded(&reply),
            "*3\r\n$4\r\ntrue\r\n$1\r\n0\r\n$1\r\n9\r\n"
        );
    }

    #[test]
    fn numbers_are_rendered_the_way_the_callers_parser_reads_them() {
        // Shortest round-trip, no exponent, fractional part only when there is one —
        // the same text `f64::to_string` produces, which the caller has always parsed.
        let reply = Reply::Bucket {
            wait: 133_333.33333333334,
            tokens: -0.4,
        };

        assert_eq!(
            encoded(&reply),
            format!(
                "*3\r\n$4\r\ntrue\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                "133333.33333333334".len(),
                "133333.33333333334",
                "-0.4".len(),
                "-0.4"
            )
        );
    }

    #[test]
    fn a_number_too_long_for_the_stack_buffer_is_still_rendered_whole() {
        let reply = Reply::Bucket {
            wait: 0.0,
            tokens: f64::MAX,
        };
        let text = f64::MAX.to_string();

        assert!(encoded(&reply).ends_with(&format!("${}\r\n{text}\r\n", text.len())));
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
