//! Turns a memory budget into an entry ceiling.
//!
//! The store's capacity and the container's memory limit are one decision, and setting
//! them separately is how a store gets killed while its own ceiling reports headroom —
//! which is exactly what the shipped defaults did before this existed. So the ceiling is
//! derived rather than configured: the container already carries a limit, and on Linux the
//! process can read it.

use std::fs;

/// Measured cost of one entry once the map has grown, with a little rounding up.
///
/// See `examples/memory_per_key`: 390 bytes at a million keys, less below that.
const BYTES_PER_ENTRY: usize = 400;

/// Share of the budget entries may occupy.
///
/// The rest goes to connection buffers — about 18KB each — the allocator's slack, and the
/// binary itself. Half is deliberately generous: being killed costs far more than holding
/// fewer keys than the machine could technically fit.
const ENTRY_SHARE: f64 = 0.5;

/// Used when no limit can be discovered, which is the usual case off Linux.
const FALLBACK_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Reads the container's memory limit, if the platform exposes one.
///
/// cgroup is a kernel facility rather than an orchestrator one, so this works the same
/// under Kubernetes, Docker and plain systemd. An unlimited or unreadable value reads as
/// absent rather than as enormous.
fn read_cgroup_limit_bytes() -> Option<usize> {
    let v2 = fs::read_to_string("/sys/fs/cgroup/memory.max").ok();
    let v1 = || fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes").ok();

    let raw = v2.or_else(v1)?;
    let trimmed = raw.trim();

    if trimmed == "max" {
        return None;
    }

    let value: usize = trimmed.parse().ok()?;

    // cgroup v1 reports a sentinel close to usize::MAX when unlimited.
    if value >= usize::MAX / 2 {
        return None;
    }

    Some(value)
}

/// The budget to size the store against, in bytes.
pub fn resolve_budget_bytes(configured_mb: Option<usize>) -> usize {
    if let Some(mb) = configured_mb {
        return mb * 1024 * 1024;
    }

    read_cgroup_limit_bytes().unwrap_or(FALLBACK_BUDGET_BYTES)
}

/// How many entries each shard may hold before the store starts trimming.
///
/// Never zero: a store that can hold nothing would trim on every insert and serve every
/// request against a full bucket, which is a rate limiter that does not limit.
pub fn entries_per_shard(budget_bytes: usize, shard_count: usize) -> usize {
    let entries = (budget_bytes as f64 * ENTRY_SHARE) as usize / BYTES_PER_ENTRY;

    (entries / shard_count.max(1)).max(1_024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configured_budget_wins() {
        assert_eq!(resolve_budget_bytes(Some(256)), 256 * 1024 * 1024);
    }

    #[test]
    fn the_shipped_limit_yields_a_ceiling_that_fits_inside_it() {
        let budget = 128 * 1024 * 1024;

        let per_shard = entries_per_shard(budget, 16);
        let worst_case = per_shard * 16 * BYTES_PER_ENTRY;

        assert!(
            worst_case < budget,
            "{worst_case} bytes of entries must fit inside a {budget} byte budget"
        );
        // And it should not be so conservative as to be useless.
        assert!(per_shard * 16 > 100_000, "got {per_shard} per shard");
    }

    #[test]
    fn a_larger_budget_holds_proportionally_more() {
        let small = entries_per_shard(128 * 1024 * 1024, 16);
        let large = entries_per_shard(512 * 1024 * 1024, 16);

        // Proportional to within the rounding that two integer divisions cost.
        assert!(large.abs_diff(small * 4) <= 16, "{small} then {large}");
    }

    #[test]
    fn a_tiny_budget_still_leaves_room_to_store_something() {
        // Below this the store would trim on every insert and stop limiting anything.
        assert_eq!(entries_per_shard(1024, 16), 1_024);
    }
}
