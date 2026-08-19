//! Structured event ids.
//!
//! Range 8000-8049 is reserved for this binary. Every entry is an error or a lifecycle
//! event on a background path; there are no success events, because a per-request log
//! on a rate-limit hot path would cost more than the request it describes.

/// The listener is accepting connections.
pub const STORE_START: (u32, &str) = (8001, "StoreStart");

/// A connection was closed because its framing could not be trusted.
pub const CONNECTION_PROTOCOL_ERROR: (u32, &str) = (8002, "ConnectionProtocolError");

/// The caller's script no longer matches the pinned text. Once per distinct text.
pub const SCRIPT_DIVERGED: (u32, &str) = (8004, "ScriptDiverged");

/// No peer answered a publish at all. A single missed peer is expected occasionally and is
/// not reported; every peer missing is a mesh that has stopped sharing.
pub const PEER_PUBLISH_FAILED: (u32, &str) = (8005, "PeerPublishFailed");

/// The configured peer endpoint resolved to no addresses, so this replica is counting
/// alone. Also emitted, once, when no endpoint is configured at all.
pub const PEER_DISCOVERY_EMPTY: (u32, &str) = (8006, "PeerDiscoveryEmpty");

/// The peer endpoint is running without a shared secret.
pub const PEER_ENDPOINT_UNAUTHENTICATED: (u32, &str) = (8007, "PeerEndpointUnauthenticated");

/// The entry ceiling derived from the memory budget at startup.
pub const STORE_CAPACITY_DERIVED: (u32, &str) = (8008, "StoreCapacityDerived");

/// The store is at its entry ceiling and shedding the least recently active keys.
pub const STORE_AT_CAPACITY: (u32, &str) = (8009, "StoreAtCapacity");

/// The listener could not accept a connection and will retry. Usually the descriptor
/// limit; never fatal.
pub const ACCEPT_FAILED: (u32, &str) = (8010, "AcceptFailed");

/// An unrecognised script arrived after the registry had reached its bound. Served, not
/// remembered.
pub const SCRIPT_REGISTRY_FULL: (u32, &str) = (8011, "ScriptRegistryFull");

/// A peer answered a report with a refusal — wrong secret, or a body it will not take.
/// Emitted when the refusals start, not per report.
pub const PEER_PUBLISH_REJECTED: (u32, &str) = (8012, "PeerPublishRejected");

/// An inbound report was refused: unauthorised, malformed, or implausibly large.
pub const PEER_REPORT_REFUSED: (u32, &str) = (8013, "PeerReportRefused");

/// A background task that must run for the process's lifetime has stopped.
pub const BACKGROUND_TASK_STOPPED: (u32, &str) = (8014, "BackgroundTaskStopped");

/// The caller sent a pinned script text for the first time; its digest is now served
/// directly. Once per revision, so an upgrade's progress can be read from the log.
pub const SCRIPT_REGISTERED: (u32, &str) = (8015, "ScriptRegistered");

/// The connection ceiling was reached; new connections are refused until one closes.
/// Emitted when the ceiling is reached, not per refused connection.
pub const CONNECTIONS_AT_CAPACITY: (u32, &str) = (8016, "ConnectionsAtCapacity");
