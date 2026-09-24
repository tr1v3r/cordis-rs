//! The service registry owned by the coordinator actor
//! (docs/04-services-events-loader.md §1–§2).
//!
//! All state here lives behind the actor's single-writer discipline: the
//! registry is only touched while handling a command or a completion, so
//! slot resolution, publication and dependency-epoch recomputation are
//! linearization points of the actor (docs/03-runtime.md §10).
//!
//! The registry owns:
//!
//! - the **slot table** `(ServiceName, ScopeId) -> BindingId`. A staged
//!   provide reserves its slot (invisible to lookups, unreachable for
//!   dependency stamps); publication swaps the reservation into the
//!   visible table. Occupied slots reject further providers — no implicit
//!   stacking (docs/04 §1.3);
//! - the **binding records**: identity, payload cell, availability and
//!   its monotonic generation, owner `(fiber, generation)` and the owning
//!   effect entry;
//! - the **reverse dependency index** `(ServiceName, ScopeId) -> FiberId`
//!   including fibers that are still `Pending` (docs/04 §2), which is how
//!   provider changes find their consumers without graph walks.

use std::any::TypeId;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::error::Error;
use crate::id::{BindingId, EffectId, FiberId, GenerationId};
use crate::plugin::AnyConfig;
use crate::services::{DepRef, LeaseCell, RequiredSlot, ScopeId, scope_sort_key};

/// One service binding as tracked by the registry.
pub(crate) struct BindingRec {
    pub(crate) id: BindingId,
    pub(crate) name: String,
    pub(crate) scope: ScopeId,
    pub(crate) type_id: TypeId,
    pub(crate) cell: std::sync::Arc<LeaseCell>,
    /// Explicit availability flag; `false` makes the binding invisible to
    /// dependency stamps and gets (docs/04 §1.2).
    pub(crate) available: bool,
    /// Monotonic counter bumped on every availability transition; part of
    /// consumers' dependency stamps, so false→true→false cycles can never
    /// make an old Starting ticket publish.
    pub(crate) availability_generation: u64,
    /// The provider `(fiber, generation)` that owns this binding.
    pub(crate) owner: (FiberId, GenerationId),
    /// The effect-ledger entry owning the binding's lifecycle.
    pub(crate) owner_effect: EffectId,
    /// Whether the binding is visible in the slot table yet.
    pub(crate) published: bool,
    /// Whether the provider ever called `set_available` explicitly;
    /// otherwise publication implies availability.
    pub(crate) explicit_availability: bool,
    /// Managed-service start state.
    pub(crate) start: StartState,
}

/// Lifecycle of a managed service's start phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartState {
    /// Plain binding: no start phase, publishable as soon as the owner
    /// commits.
    NoStartRequired,
    /// Managed: the start worker is in flight; the binding must not
    /// publish.
    StartPending,
    /// Managed: the start worker succeeded; publishable.
    StartSucceeded,
    /// Managed: the start worker failed or was aborted; the binding is
    /// being retired.
    StartFailed,
}

impl BindingRec {
    /// Whether this binding may become visible now.
    pub(crate) fn publishable(&self) -> bool {
        matches!(
            self.start,
            StartState::NoStartRequired | StartState::StartSucceeded
        )
    }
}

/// Slot key of the registry tables.
pub(crate) type SlotKey = (String, ScopeId);

/// The service registry (docs/04 §1–§2). Actor-confined.
pub(crate) struct ServiceRegistry {
    slots: HashMap<SlotKey, BindingId>,
    bindings: HashMap<BindingId, BindingRec>,
    reverse: HashMap<SlotKey, HashSet<FiberId>>,
    next_seq: u64,
}

impl ServiceRegistry {
    pub(crate) fn new() -> Self {
        Self {
            slots: HashMap::new(),
            bindings: HashMap::new(),
            reverse: HashMap::new(),
            next_seq: 0,
        }
    }

    fn alloc_binding_id(&mut self, owner: (FiberId, GenerationId)) -> BindingId {
        self.next_seq += 1;
        BindingId::new(owner.0, owner.1, self.next_seq)
    }

    /// Reserves `slot` for a new binding of `value` under `owner`
    /// (docs/04 §1.3: staged provide reserves the slot; duplicates are
    /// refused, never stacked).
    pub(crate) fn stage(
        &mut self,
        slot: SlotKey,
        type_id: TypeId,
        value: AnyConfig,
        owner: (FiberId, GenerationId),
        owner_effect: EffectId,
        start: StartState,
    ) -> Result<BindingId, Error> {
        if self.slots.contains_key(&slot) {
            return Err(Error::ServiceExists {
                service: slot.0.clone(),
                namespace: slot.1.as_str(),
            });
        }
        let id = self.alloc_binding_id(owner);
        let rec = BindingRec {
            id,
            name: slot.0.clone(),
            scope: slot.1.clone(),
            type_id,
            cell: std::sync::Arc::new(LeaseCell::new(value)),
            // A staged binding starts unavailable: explicit publication is
            // what makes it visible (either `true` here once published, or
            // carried over from the provider's explicit availability).
            available: false,
            availability_generation: 0,
            owner,
            owner_effect,
            published: false,
            explicit_availability: false,
            start,
        };
        self.slots.insert(slot, id);
        self.bindings.insert(id, rec);
        Ok(id)
    }

    /// Looks a binding up by identity.
    pub(crate) fn get(&self, binding: &BindingId) -> Option<&BindingRec> {
        self.bindings.get(binding)
    }

    /// Mutable access by identity.
    pub(crate) fn get_mut(&mut self, binding: &BindingId) -> Option<&mut BindingRec> {
        self.bindings.get_mut(binding)
    }

    /// The binding currently visible in `slot`, if any.
    pub(crate) fn published(&self, slot: &SlotKey) -> Option<&BindingRec> {
        let id = self.slots.get(slot)?;
        let rec = self.bindings.get(id)?;
        rec.published.then_some(rec)
    }

    /// The binding occupying `slot`, staged or published (provider-side
    /// operations address their own staged bindings too).
    pub(crate) fn binding_at(&self, slot: &SlotKey) -> Option<BindingId> {
        self.slots.get(slot).copied()
    }

    /// Publishes a staged binding if it is publishable; returns the slot
    /// when the visibility of the slot changed.
    ///
    /// A provider's explicit availability set while staged is honored
    /// here; bindings that never called `set_available` default to
    /// available at publication.
    pub(crate) fn publish(&mut self, binding: &BindingId) -> Option<SlotKey> {
        let rec = self.bindings.get_mut(binding)?;
        if rec.published || !rec.publishable() {
            return None;
        }
        rec.published = true;
        if !rec.explicit_availability {
            rec.available = true;
        }
        Some((rec.name.clone(), rec.scope.clone()))
    }

    /// Retires a binding: frees the slot (when it still points at this
    /// binding), unpublishes it from discovery and extracts the payload
    /// for off-actor drop (D22). Returns the extracted value.
    pub(crate) fn retire(&mut self, binding: &BindingId) -> Option<AnyConfig> {
        let rec = self.bindings.remove(binding)?;
        let slot = (rec.name.clone(), rec.scope.clone());
        if self.slots.get(&slot) == Some(binding) {
            self.slots.remove(&slot);
        }
        rec.cell.retire()
    }

    /// Replaces the payload of a live binding, bumping the value
    /// revision. Value changes never touch availability or the dependency
    /// epoch (docs/04 §1.2: `set` does not reload consumers). Returns
    /// `false` when the binding no longer exists.
    pub(crate) fn set_value(&mut self, binding: &BindingId, value: AnyConfig) -> bool {
        match self.bindings.get(binding) {
            Some(rec) => {
                rec.cell.set(value);
                true
            }
            None => false,
        }
    }

    /// Records an explicit availability transition and returns the new
    /// availability generation plus the slot when the transition happened
    /// on a published binding (invisible staged bindings cannot have
    /// dependents).
    pub(crate) fn set_available(
        &mut self,
        binding: &BindingId,
        available: bool,
    ) -> Option<(u64, SlotKey)> {
        let rec = self.bindings.get_mut(binding)?;
        rec.explicit_availability = true;
        rec.available = available;
        rec.availability_generation += 1;
        let generation = rec.availability_generation;
        rec.published
            .then(|| (generation, (rec.name.clone(), rec.scope.clone())))
    }

    /// Registers `fiber` as a consumer of `slot` (reverse index, including
    /// Pending fibers).
    pub(crate) fn add_dependent(&mut self, slot: &SlotKey, fiber: FiberId) {
        self.reverse.entry(slot.clone()).or_default().insert(fiber);
    }

    /// Removes `fiber` from one slot's consumer set.
    pub(crate) fn remove_dependent(&mut self, slot: &SlotKey, fiber: FiberId) {
        if let Some(set) = self.reverse.get_mut(slot) {
            set.remove(&fiber);
            if set.is_empty() {
                self.reverse.remove(slot);
            }
        }
    }

    /// The consumers of a slot (owned copies; caller mutates fibers).
    pub(crate) fn dependents_of(&self, slot: &SlotKey) -> Vec<FiberId> {
        self.reverse
            .get(slot)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Retires every binding owned by `fiber` (safety sweep at terminal
    /// states) and returns the extracted values plus the changed slots.
    pub(crate) fn retire_owned_by(&mut self, fiber: FiberId) -> (Vec<AnyConfig>, Vec<SlotKey>) {
        let owned: Vec<BindingId> = self
            .bindings
            .values()
            .filter(|rec| rec.owner.0 == fiber)
            .map(|rec| rec.id)
            .collect();
        let mut values = Vec::new();
        let mut slots = Vec::new();
        for binding in owned {
            let slot = self
                .bindings
                .get(&binding)
                .map(|rec| (rec.name.clone(), rec.scope.clone()));
            if let Some(value) = self.retire(&binding) {
                values.push(value);
            }
            if let Some(slot) = slot {
                slots.push(slot);
            }
        }
        (values, slots)
    }

    /// The number of live bindings (diagnostics).
    pub(crate) fn bindings_live(&self) -> usize {
        self.bindings.len()
    }

    /// Retires every binding, returning all extracted payloads (the
    /// best-effort teardown path). Slots and records are cleared.
    pub(crate) fn take_all(&mut self) -> Vec<AnyConfig> {
        let ids: Vec<BindingId> = self.bindings.keys().copied().collect();
        let mut values = Vec::new();
        for id in ids {
            if let Some(value) = self.retire(&id) {
                values.push(value);
            }
        }
        self.slots.clear();
        self.reverse.clear();
        values
    }
}

/// The dependency world of one fiber, as computed from the registry
/// (docs/04 §1.2).
#[derive(Debug, Clone, Default)]
pub(crate) struct DepFacts {
    /// All required slots resolve to published, available bindings.
    pub(crate) ready: bool,
    /// Stable hash of the dependency world: `Unavailable{reasons}` and the
    /// empty-ready vector are never conflated (docs/04 §1.2).
    pub(crate) stamp: u64,
    /// Human-readable reasons while not ready (include namespaces).
    pub(crate) missing: Vec<String>,
    /// The pinned `(BindingId, availability generation)` vector.
    pub(crate) refs: Vec<DepRef>,
}

impl DepFacts {
    /// Builds the reducer context view of these facts.
    pub(crate) fn converge_ctx(&self, parent_ready: Option<bool>) -> crate::machine::ConvergeCtx {
        crate::machine::ConvergeCtx {
            parent_ready,
            deps_ready: self.ready,
            dep_stamp: self.stamp,
            dep_missing: self.missing.clone(),
        }
    }
}

/// FNV-1a over bytes; deterministic within and across processes.
fn fnv_write(state: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *state ^= u64::from(*byte);
        *state = state.wrapping_mul(0x100_0000_01b3);
    }
}

/// Computes the dependency facts of `required` against `registry`.
///
/// The pinned vector is built in `(name, scope)` order, so the stamp is
/// independent of declaration order. A required slot that resolves to a
/// binding of the wrong type stays **not ready** with an explicit reason
/// (V24: type confusion is never silently skipped).
pub(crate) fn compute_dep_facts(registry: &ServiceRegistry, required: &[RequiredSlot]) -> DepFacts {
    let mut ordered: BTreeMap<(String, (u8, u64, String)), &RequiredSlot> = BTreeMap::new();
    for slot in required {
        let key = (slot.name.clone(), scope_sort_key(&slot.scope));
        ordered.insert(key, slot);
    }

    let mut refs = Vec::new();
    let mut missing = Vec::new();
    for ((name, _), slot) in &ordered {
        let namespace = slot.scope.as_str();
        match registry.published(&(name.clone(), slot.scope.clone())) {
            Some(binding) if binding.type_id != slot.type_id => missing.push(format!(
                "service {name:?} is bound with a different type in namespace {namespace}"
            )),
            Some(binding) if !binding.available => missing.push(format!(
                "service {name:?} is provided but not available in namespace {namespace}"
            )),
            Some(binding) => refs.push(DepRef {
                name: name.clone(),
                scope: slot.scope.clone(),
                binding: binding.id,
                availability_generation: binding.availability_generation,
            }),
            None => missing.push(format!(
                "service {name:?} is not provided in namespace {namespace}"
            )),
        }
    }

    let mut stamp: u64 = 0xcbf2_9ce4_8422_2325;
    fnv_write(
        &mut stamp,
        if refs.is_empty() && missing.is_empty() {
            b"ready:empty\x00"
        } else if missing.is_empty() {
            b"ready:\x00"
        } else {
            b"unavailable:\x00"
        },
    );
    if missing.is_empty() {
        for dep in &refs {
            let binding = dep.binding.seq();
            let avail = dep.availability_generation;
            fnv_write(&mut stamp, &binding.to_le_bytes());
            fnv_write(&mut stamp, &avail.to_le_bytes());
        }
    } else {
        // Sort the reasons so the stamp depends on the set, not order.
        let mut reasons = missing.clone();
        reasons.sort();
        for reason in &reasons {
            fnv_write(&mut stamp, reason.as_bytes());
            fnv_write(&mut stamp, b"\x00");
        }
    }

    DepFacts {
        ready: missing.is_empty(),
        stamp,
        missing,
        refs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{EffectId, FiberId, GenerationId};
    use crate::services::RequiredSlot;
    use std::sync::Arc;

    struct Db {
        #[expect(dead_code, reason = "payload shape is irrelevant here")]
        url: String,
    }

    struct Cache;

    fn new_owner() -> (FiberId, GenerationId) {
        (FiberId::alloc_global(), GenerationId::alloc_global())
    }

    fn slot(name: &str, scope: ScopeId) -> RequiredSlot {
        RequiredSlot {
            name: name.to_owned(),
            scope,
            type_id: TypeId::of::<Db>(),
        }
    }

    #[test]
    fn staging_reserves_slots_and_rejects_duplicates() {
        let mut registry = ServiceRegistry::new();
        let owner = new_owner();
        let slot = ("db".to_owned(), ScopeId::Default);
        let first = registry
            .stage(
                slot.clone(),
                TypeId::of::<Db>(),
                Arc::new(Db { url: String::new() }),
                owner,
                EffectId::alloc_global(),
                StartState::NoStartRequired,
            )
            .expect("first stage");

        let second = registry.stage(
            slot.clone(),
            TypeId::of::<Db>(),
            Arc::new(Db { url: String::new() }),
            new_owner(),
            EffectId::alloc_global(),
            StartState::NoStartRequired,
        );
        assert!(matches!(second, Err(Error::ServiceExists { .. })));

        // Staged bindings are invisible.
        assert!(registry.published(&slot).is_none());
        // A different slot does not collide (same label, other service).
        let other = ("cache".to_owned(), ScopeId::Shared("team".to_owned()));
        assert!(
            registry
                .stage(
                    other,
                    TypeId::of::<Cache>(),
                    Arc::new(Cache),
                    new_owner(),
                    EffectId::alloc_global(),
                    StartState::NoStartRequired
                )
                .is_ok()
        );

        // Publication makes it visible and frees only via retire.
        let changed = registry.publish(&first).expect("published");
        assert_eq!(changed, slot);
        assert!(registry.published(&slot).is_some());
        let value = registry.retire(&first).expect("value out");
        assert!(value.downcast::<Db>().is_ok());
        assert!(registry.published(&slot).is_none());
        assert_eq!(registry.bindings_live(), 1, "cache binding remains");
    }

    #[test]
    fn availability_transitions_bump_generations_only_when_published() {
        let mut registry = ServiceRegistry::new();
        let owner = new_owner();
        let slot = ("db".to_owned(), ScopeId::Default);
        let binding = registry
            .stage(
                slot.clone(),
                TypeId::of::<Db>(),
                Arc::new(Db { url: String::new() }),
                owner,
                EffectId::alloc_global(),
                StartState::NoStartRequired,
            )
            .unwrap();

        // Staged: transition happens but no slot notification is needed.
        assert!(registry.set_available(&binding, false).is_none());
        registry.publish(&binding).unwrap();
        // Explicitly-unavailable binding publishes invisible.
        assert!(!registry.published(&slot).unwrap().available);

        let (generation, notified) = registry.set_available(&binding, true).unwrap();
        assert_eq!(notified, slot);
        assert!(registry.published(&slot).unwrap().available);
        assert_eq!(
            registry.published(&slot).unwrap().availability_generation,
            generation
        );
        assert_eq!(generation, 2, "one bump per transition");
    }

    #[test]
    fn dep_facts_distinguish_missing_empty_and_ready() {
        let mut registry = ServiceRegistry::new();
        let provider = new_owner();

        // Empty requirements: ready with a distinct stamp.
        let empty = compute_dep_facts(&registry, &[]);
        assert!(empty.ready);
        assert_eq!(empty.refs.len(), 0);

        // Missing requirement: unavailable, reason carries the namespace.
        let missing = compute_dep_facts(&registry, &[slot("db", ScopeId::Default)]);
        assert!(!missing.ready);
        assert_eq!(missing.missing.len(), 1);
        assert!(missing.missing[0].contains("not provided"));
        assert!(missing.missing[0].contains("default"));
        assert_ne!(missing.stamp, empty.stamp, "missing != empty-ready");

        // Provide and publish: ready with a pinned ref.
        let binding = registry
            .stage(
                ("db".to_owned(), ScopeId::Default),
                TypeId::of::<Db>(),
                Arc::new(Db { url: String::new() }),
                provider,
                EffectId::alloc_global(),
                StartState::NoStartRequired,
            )
            .unwrap();
        registry.publish(&binding).unwrap();
        let ready = compute_dep_facts(&registry, &[slot("db", ScopeId::Default)]);
        assert!(ready.ready);
        assert_eq!(ready.refs.len(), 1);
        assert_ne!(ready.stamp, missing.stamp);

        // Value changes keep the stamp (V21) …
        assert!(registry.set_value(
            &binding,
            Arc::new(Db {
                url: "new".to_owned()
            })
        ));
        let after_set = compute_dep_facts(&registry, &[slot("db", ScopeId::Default)]);
        assert_eq!(after_set.stamp, ready.stamp);

        // … availability transitions change it (V26).
        registry.set_available(&binding, false).unwrap();
        let unavailable = compute_dep_facts(&registry, &[slot("db", ScopeId::Default)]);
        assert!(!unavailable.ready);
        registry.set_available(&binding, true).unwrap();
        let recovered = compute_dep_facts(&registry, &[slot("db", ScopeId::Default)]);
        assert!(recovered.ready);
        assert_ne!(
            recovered.stamp, ready.stamp,
            "false->true must bump the stamp"
        );

        // A staged (unpublished) replacement is invisible even in the
        // same slot: the old binding's retirement removed visibility.
        registry.retire(&binding);
        let retired = compute_dep_facts(&registry, &[slot("db", ScopeId::Default)]);
        assert!(!retired.ready);
        assert_eq!(retired.stamp, missing.stamp, "same world, same stamp");
    }

    #[test]
    fn dep_facts_report_type_confusion_as_missing() {
        let mut registry = ServiceRegistry::new();
        let binding = registry
            .stage(
                ("db".to_owned(), ScopeId::Default),
                TypeId::of::<Cache>(),
                Arc::new(Cache),
                new_owner(),
                EffectId::alloc_global(),
                StartState::NoStartRequired,
            )
            .unwrap();
        registry.publish(&binding).unwrap();

        let facts = compute_dep_facts(&registry, &[slot("db", ScopeId::Default)]);
        assert!(!facts.ready);
        assert!(facts.missing[0].contains("different type"));
    }

    #[test]
    fn stamp_is_independent_of_declaration_order() {
        let mut registry = ServiceRegistry::new();
        let owner = new_owner();
        let db = registry
            .stage(
                ("db".to_owned(), ScopeId::Default),
                TypeId::of::<Db>(),
                Arc::new(Db { url: String::new() }),
                owner,
                EffectId::alloc_global(),
                StartState::NoStartRequired,
            )
            .unwrap();
        registry.publish(&db).unwrap();
        let cache = registry
            .stage(
                ("cache".to_owned(), ScopeId::Shared("t".to_owned())),
                TypeId::of::<Cache>(),
                Arc::new(Cache),
                owner,
                EffectId::alloc_global(),
                StartState::NoStartRequired,
            )
            .unwrap();
        registry.publish(&cache).unwrap();

        let a = compute_dep_facts(
            &registry,
            &[
                slot("db", ScopeId::Default),
                RequiredSlot {
                    name: "cache".to_owned(),
                    scope: ScopeId::Shared("t".to_owned()),
                    type_id: TypeId::of::<Cache>(),
                },
            ],
        );
        let b = compute_dep_facts(
            &registry,
            &[
                RequiredSlot {
                    name: "cache".to_owned(),
                    scope: ScopeId::Shared("t".to_owned()),
                    type_id: TypeId::of::<Cache>(),
                },
                slot("db", ScopeId::Default),
            ],
        );
        assert_eq!(a.stamp, b.stamp);
        assert_eq!(a.refs, b.refs);
    }
}
