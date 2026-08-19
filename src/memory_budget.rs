//! Turns a memory budget into an entry ceiling.
//!
//! The store's capacity and the container's memory limit are one decision, and setting
//! them separately is how a store gets killed while its own ceiling reports headroom —
//! which is exactly what the shipped defaults did before this existed. So the ceiling is
//! derived rather than configured: the container already carries a limit, and on Linux the
//! process can read it.

use std::fs;

/// Share of the budget the shard tables may occupy.
///
/// The rest goes to connection buffers (about 15KB each at full concurrency, measured),
/// the allocator's slack, the peer reports in flight, and the binary itself. Half is
/// deliberately generous: being killed costs far more than holding fewer keys than the
/// machine could technically fit.
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
/// Each shard's table is allocated once, at a power-of-two number of slots, and never
/// grows. So the ceiling is chosen the other way round: the largest table whose slots fit
/// the shard's share of the budget, filled to the seven-eighths the map allows. The figure
/// planned for is then the figure allocated — measured at 170 bytes per key on Linux with
/// growing tables, and exactly `BYTES_PER_SLOT` per slot with these.
///
/// Never below a floor: a store that can hold nothing would trim on every insert and serve
/// every request against a full bucket, which is a rate limiter that does not limit.
pub fn derive_entries_per_shard(budget_bytes: usize, shard_count: usize) -> usize {
    let share_per_shard = (budget_bytes as f64 * ENTRY_SHARE) as usize / shard_count.max(1);
    let slots_that_fit = share_per_shard / crate::store::BYTES_PER_SLOT;

    let mut slots = 1usize;
    while slots * 2 <= slots_that_fit {
        slots *= 2;
    }

    (slots * 7 / 8).max(1_024)
}

/// What the shard tables will occupy for a ceiling: the figure to log beside it.
pub fn compute_table_bytes(entries_per_shard: usize, shard_count: usize) -> usize {
    crate::store::count_slots_for(entries_per_shard) * crate::store::BYTES_PER_SLOT * shard_count
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

        let per_shard = derive_entries_per_shard(budget, 16);
        let allocated = compute_table_bytes(per_shard, 16);

        assert!(
            allocated <= budget / 2,
            "{allocated} bytes of tables must fit the entries' half of a {budget} byte budget"
        );
        // And it should not be so conservative as to be useless.
        assert!(per_shard * 16 > 100_000, "got {per_shard} per shard");
    }

    #[test]
    fn the_ceiling_fills_the_table_it_allocates() {
        // The table is a power of two of slots; the ceiling is seven-eighths of it, which is
        // exactly the load the map grows at. Planning for less would waste the slots;
        // planning for more would make the map grow under a shard's lock.
        let per_shard = derive_entries_per_shard(128 * 1024 * 1024, 16);

        assert_eq!(crate::store::count_slots_for(per_shard) * 7 / 8, per_shard);
    }

    #[test]
    fn a_larger_budget_holds_more() {
        let small = derive_entries_per_shard(128 * 1024 * 1024, 16);
        let large = derive_entries_per_shard(512 * 1024 * 1024, 16);

        // Four times the budget is four times the slots: two doublings.
        assert_eq!(large, small * 4, "{small} then {large}");
    }

    #[test]
    fn a_tiny_budget_still_leaves_room_to_store_something() {
        // Below this the store would trim on every insert and stop limiting anything.
        assert_eq!(derive_entries_per_shard(1024, 16), 1_024);
    }
}
