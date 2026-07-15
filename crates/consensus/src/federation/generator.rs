use super::{
    FederationRoot, GeneratorLease, GeneratorLeaseState, GeneratorReleaseProof, GlobalGroupId,
};

impl FederationRoot {
    pub fn allocate_generator_slot(
        &mut self,
        holder: GlobalGroupId,
        now_ms: i64,
    ) -> Result<GeneratorLease, String> {
        self.reclaim_generator_slots(now_ms);
        if let Some(existing) = self
            .generator_leases
            .values()
            .find(|lease| lease.holder == holder && lease.state == GeneratorLeaseState::Active)
        {
            return Ok(existing.clone());
        }
        let range = self
            .generator_ranges
            .get(&holder.cell)
            .copied()
            .or_else(|| {
                (self.cells.len() <= 1).then_some(super::GeneratorSlotRange {
                    start: 1,
                    end: u16::MAX,
                })
            })
            .ok_or_else(|| "Home Cell has no Root generator slot grant".to_owned())?;
        let slot = (range.start..=range.end)
            .find(|slot| !self.generator_leases.contains_key(slot))
            .ok_or_else(|| "wire message generator slot space is exhausted".to_owned())?;
        let incarnation = self.next_generator_incarnation.max(1);
        self.next_generator_incarnation = incarnation.saturating_add(1);
        let lease = GeneratorLease {
            slot,
            holder,
            incarnation,
            state: GeneratorLeaseState::Active,
            quarantine_until_ms: None,
        };
        self.generator_leases.insert(slot, lease.clone());
        self.bump_epoch();
        Ok(lease)
    }

    pub fn release_generator_slot(
        &mut self,
        slot: u16,
        proof: GeneratorReleaseProof,
        now_ms: i64,
        quarantine_ms: i64,
    ) -> Result<(), String> {
        if !proof.is_clear() {
            return Err("wire slot still has message lifetime references".into());
        }
        let lease = self
            .generator_leases
            .get_mut(&slot)
            .ok_or_else(|| "wire slot is not leased".to_owned())?;
        if lease.state != GeneratorLeaseState::Active {
            return Err("wire slot is already quarantined".into());
        }
        lease.state = GeneratorLeaseState::Quarantined;
        lease.quarantine_until_ms = Some(now_ms.saturating_add(quarantine_ms.max(1)));
        self.bump_epoch();
        Ok(())
    }

    pub fn reclaim_generator_slots(&mut self, now_ms: i64) -> usize {
        let before = self.generator_leases.len();
        self.generator_leases.retain(|_, lease| {
            lease.state != GeneratorLeaseState::Quarantined
                || lease.quarantine_until_ms.is_none_or(|until| until > now_ms)
        });
        let removed = before - self.generator_leases.len();
        if removed > 0 {
            self.bump_epoch();
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CellId;

    #[test]
    fn wire_slot_requires_full_lifetime_clearance_and_quarantine() {
        let mut root = FederationRoot::default();
        let group = GlobalGroupId::new(CellId(1), 7).unwrap();
        let lease = root.allocate_generator_slot(group, 10).unwrap();
        let blocked = GeneratorReleaseProof {
            in_flight: 1,
            ..GeneratorReleaseProof::default()
        };
        assert!(root
            .release_generator_slot(lease.slot, blocked, 20, 100)
            .is_err());
        root.release_generator_slot(lease.slot, GeneratorReleaseProof::default(), 20, 100)
            .unwrap();
        assert_eq!(root.reclaim_generator_slots(119), 0);
        assert_eq!(root.reclaim_generator_slots(120), 1);
        let next = root.allocate_generator_slot(group, 121).unwrap();
        assert_eq!(next.slot, lease.slot);
        assert_ne!(next.incarnation, lease.incarnation);
    }
}
