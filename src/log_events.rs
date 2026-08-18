//! Structured event ids.
//!
//! Range 8000-8049 is reserved for this binary. Every entry is an error or a lifecycle
//! event on a background path; there are no success events, because a per-request log
//! on a rate-limit hot path would cost more than the request it describes.

/// The listener is accepting connections.
pub const STORE_START: (u32, &str) = (8001, "StoreStart");

/// A connection was closed because its framing could not be trusted.
pub const CONNECTION_PROTOCOL_ERROR: (u32, &str) = (8002, "ConnectionProtocolError");

/// A connection ended with a transport failure rather than a clean close.
pub const CONNECTION_TRANSPORT_ERROR: (u32, &str) = (8003, "ConnectionTransportError");

/// The caller's script no longer matches the pinned text.
pub const SCRIPT_DIVERGED: (u32, &str) = (8004, "ScriptDiverged");

/// A peer report could not be delivered. Expected occasionally; the peer ages out.
pub const PEER_PUBLISH_FAILED: (u32, &str) = (8005, "PeerPublishFailed");

/// Peer discovery returned no addresses, so this replica is counting alone.
pub const PEER_DISCOVERY_EMPTY: (u32, &str) = (8006, "PeerDiscoveryEmpty");

/// The peer endpoint is running without a shared secret.
pub const PEER_ENDPOINT_UNAUTHENTICATED: (u32, &str) = (8007, "PeerEndpointUnauthenticated");

/// The entry ceiling derived from the memory budget at startup.
pub const STORE_CAPACITY_DERIVED: (u32, &str) = (8008, "StoreCapacityDerived");

/// The store is at its entry ceiling and shedding the least recently active keys.
pub const STORE_AT_CAPACITY: (u32, &str) = (8009, "StoreAtCapacity");
