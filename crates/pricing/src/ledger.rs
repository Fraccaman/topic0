//! Spend accounting: accumulates units + money, exposes remaining free quota.
//! Thread-safe via atomics (shared writer/reader).

use shared::{CostUnits, MicroUsd};
use std::sync::atomic::{AtomicU64, Ordering};

/// Per-source spend ledger with an optional monthly free-quota guard.
///
/// Tracks two money figures: `spent_micro_usd` is the raw marginal cost (every unit
/// priced), while `billable_micro_usd` charges only units beyond the free allotment —
/// $0 until `monthly_quota` is exhausted, overage after.
#[derive(Debug)]
pub struct SpendLedger {
    units: AtomicU64,
    micro_usd: AtomicU64,
    monthly_quota: Option<u64>,
    /// Money per unit as `(numerator, denominator)` micro-USD (from the `CostModel`).
    rate: (u64, u64),
}

impl SpendLedger {
    pub fn new(monthly_quota: Option<u64>, rate: (u64, u64)) -> Self {
        Self {
            units: AtomicU64::new(0),
            micro_usd: AtomicU64::new(0),
            monthly_quota,
            rate,
        }
    }

    /// Record a spend event.
    pub fn record(&self, units: CostUnits, money: MicroUsd) {
        self.units.fetch_add(units.0, Ordering::Relaxed);
        self.micro_usd.fetch_add(money.0, Ordering::Relaxed);
    }

    pub fn spent_units(&self) -> CostUnits {
        CostUnits(self.units.load(Ordering::Relaxed))
    }

    pub fn spent_micro_usd(&self) -> MicroUsd {
        MicroUsd(self.micro_usd.load(Ordering::Relaxed))
    }

    /// Billable money: only units past the free allotment are charged (no quota = bill
    /// all). 0 while inside the monthly free tier.
    pub fn billable_micro_usd(&self) -> MicroUsd {
        let units = self.units.load(Ordering::Relaxed);
        let free = self.monthly_quota.unwrap_or(0);
        let (num, den) = self.rate;
        MicroUsd(units.saturating_sub(free) * num / den.max(1))
    }

    /// Remaining free units before the monthly quota is blown (None = unlimited).
    pub fn remaining_quota(&self) -> Option<u64> {
        self.monthly_quota
            .map(|q| q.saturating_sub(self.units.load(Ordering::Relaxed)))
    }

    /// True if a free quota exists and is exhausted.
    pub fn quota_exhausted(&self) -> bool {
        matches!(self.remaining_quota(), Some(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_and_guards_quota() {
        // rate 1.2 micro-USD/unit, 100-unit free allotment.
        let l = SpendLedger::new(Some(100), (12, 10));
        l.record(CostUnits(40), MicroUsd(48));
        assert_eq!(l.spent_units().0, 40);
        assert_eq!(l.remaining_quota(), Some(60));
        assert!(!l.quota_exhausted());
        // Inside the free tier: raw cost accrues, billable is 0.
        assert_eq!(l.spent_micro_usd().0, 48);
        assert_eq!(l.billable_micro_usd().0, 0);
        l.record(CostUnits(60), MicroUsd(72));
        assert_eq!(l.remaining_quota(), Some(0));
        assert!(l.quota_exhausted());
        assert_eq!(l.billable_micro_usd().0, 0);
        // 50 units past the allotment → billed at the marginal rate.
        l.record(CostUnits(50), MicroUsd(60));
        assert_eq!(l.billable_micro_usd().0, 50 * 12 / 10);
    }

    #[test]
    fn no_quota_bills_every_unit() {
        let l = SpendLedger::new(None, (12, 10));
        l.record(CostUnits(100), MicroUsd(120));
        assert_eq!(l.billable_micro_usd().0, 120);
    }
}
