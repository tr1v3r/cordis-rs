//! Identity newtypes for the cordis-rs kernel.
//!
//! Every id is a plain numeric handle allocated from a process-global
//! monotonic counter, so ids never collide across apps. Ids identify
//! entities; they never embed or reveal configuration content, so their
//! [`Debug`](std::fmt::Debug) output is safe to log.
//!
//! App isolation (V03) does **not** come from namespacing ids per app: it
//! comes from routing every operation through the owning app, which every
//! [`Context`](crate::Context) and [`FiberHandle`](crate::FiberHandle)
//! holds as a weak reference. A handle of app A can therefore never
//! resolve state inside app B, and identical numeric values across apps
//! cannot happen in the first place because the allocators are global.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global allocator for definition identities.
static NEXT_DEFINITION_ID: AtomicU64 = AtomicU64::new(1);
/// Global allocator for runtime identities.
static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);
/// Global allocator for fiber identities.
static NEXT_FIBER_ID: AtomicU64 = AtomicU64::new(1);
/// Global allocator for generation identities.
static NEXT_GENERATION_ID: AtomicU64 = AtomicU64::new(1);
/// Global allocator for operation identities.
static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

/// Declares an id newtype over a `u64` counter value.
///
/// The generated `Debug` prints only the type name and the numeric id —
/// never configuration content. Equality, ordering and hashing are based on
/// the internal id alone.
macro_rules! id_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            /// Returns the raw numeric value of this id.
            ///
            /// Diagnostics only: the number carries no configuration data.
            pub fn as_u64(self) -> u64 {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }
    };
}

id_newtype!(
    /// Identity of a plugin definition, allocated once per [`define`](crate::define)
    /// call.
    ///
    /// Cloning a `Plugin` keeps the same `DefinitionId`; calling `define`
    /// again — even with the same name and config type — allocates a fresh
    /// one (V02).
    DefinitionId
);

id_newtype!(
    /// Identity of one loaded fiber: a single configuration-bearing instance
    /// of a definition. One runtime can own many fibers (V01).
    FiberId
);

id_newtype!(
    /// Identity of one activation generation of a fiber.
    ///
    /// Every (re)start of a fiber creates a new generation; results and
    /// registrations from an old generation must not leak into a newer one.
    /// Wired into the lifecycle in P2.
    GenerationId
);

id_newtype!(
    /// Identity of a registered effect entry (listener, disposer, child
    /// scope, task). Managed by the effect ledger from P3.
    EffectId
);

id_newtype!(
    /// Identity of one service binding: a provider instance publishing a
    /// service into a scope. Managed by the service registry from P4.
    BindingId
);

id_newtype!(
    /// Identity of one lifecycle operation receipt (load, update, restart,
    /// dispose). Issued by the coordinator from P2.
    OperationId
);

id_newtype!(
    /// Identity of a plugin runtime: the per-app state shared by every fiber
    /// loaded from the same definition.
    ///
    /// Distinct from [`DefinitionId`]: a definition keeps its identity even
    /// after its runtime is disposed, while a fresh load creates a new
    /// `RuntimeId`. This keeps already-retired ids detectable (V03).
    RuntimeId
);

impl DefinitionId {
    /// Allocates the next process-global definition identity.
    pub(crate) fn alloc_global() -> Self {
        Self(NEXT_DEFINITION_ID.fetch_add(1, Ordering::Relaxed))
    }
}

/// Definition identity reserved for the app's internal root fiber
/// (docs/03-runtime.md §9). The root runs no plugin code; the reserved id
/// only keeps its records shaped like every other fiber. It can never
/// collide with user definitions because the global allocator starts at 1.
pub(crate) const ROOT_DEFINITION_ID: DefinitionId = DefinitionId(0);

impl RuntimeId {
    /// Allocates the next process-global runtime identity.
    pub(crate) fn alloc_global() -> Self {
        Self(NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl FiberId {
    /// Allocates the next process-global fiber identity.
    pub(crate) fn alloc_global() -> Self {
        Self(NEXT_FIBER_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl GenerationId {
    /// Allocates the next process-global generation identity.
    ///
    /// Called by the coordinator when it assigns a new generation to a
    /// fiber; generation tokens handed to workers always originate here.
    pub(crate) fn alloc_global() -> Self {
        Self(NEXT_GENERATION_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl OperationId {
    /// Allocates the next process-global operation identity.
    ///
    /// Called by the coordinator when it admits a lifecycle command and
    /// issues the operation receipt returned to the caller.
    pub(crate) fn alloc_global() -> Self {
        Self(NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn hash_of<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn ids_allocate_distinct_values() {
        let first = FiberId::alloc_global();
        let second = FiberId::alloc_global();
        assert_ne!(first, second);

        let definition_a = DefinitionId::alloc_global();
        let definition_b = DefinitionId::alloc_global();
        assert_ne!(definition_a, definition_b);

        let runtime_a = RuntimeId::alloc_global();
        let runtime_b = RuntimeId::alloc_global();
        assert_ne!(runtime_a, runtime_b);
    }

    #[test]
    fn equality_and_hash_follow_the_internal_id() {
        let id = FiberId::alloc_global();
        let copy = id;

        assert_eq!(id, copy);
        assert_eq!(hash_of(&id), hash_of(&copy));
        assert_eq!(id.as_u64(), copy.as_u64());
    }

    #[test]
    fn debug_prints_only_the_numeric_id() {
        let id = FiberId::alloc_global();
        let rendered = format!("{id:?}");
        assert!(rendered.starts_with("FiberId("));
        assert!(rendered.ends_with(&format!("{})", id.as_u64())));

        let runtime = RuntimeId::alloc_global();
        let rendered = format!("{runtime:?}");
        assert!(rendered.starts_with("RuntimeId("));
        assert!(rendered.ends_with(')'));
    }
}
