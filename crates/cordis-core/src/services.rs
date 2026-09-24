//! The P4 service layer: typed keys, leases and namespace scopes
//! (docs/02-api.md §5, docs/04-services-events-loader.md §1).
//!
//! Ownership and visibility rules implemented on top of these types by
//! the coordinator's service registry:
//!
//! - the registry indexes slots by `(ServiceName, ScopeId)`; the expected
//!   [`TypeId`] of a [`ServiceKey`] is a *check*, never part of the index,
//!   so two callers of different types can never see two "same-named"
//!   services;
//! - a binding is identified by [`BindingId`] and carries a value cell
//!   shared with outstanding leases: `set` swaps the value and bumps the
//!   value revision *without* touching the dependency epoch, while
//!   availability changes bump the availability generation *without*
//!   changing the value;
//! - a [`ServiceLease`] pins one binding: snapshots observe `set` updates
//!   and are refused with [`Error::ServiceRetired`] once the binding is
//!   retired — but arcs already taken out are never revoked in place.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use crate::error::Error;
use crate::id::BindingId;
use crate::plugin::AnyConfig;

/// Typed handle naming one service (docs/02-api.md §5).
///
/// The name selects the registry slot; `T` is the expected payload type
/// and is only used for type checks at get/provide time. Cloning a key
/// keeps the same name; keys of the same name but different types address
/// the same slot and the mismatch surfaces as
/// [`Error::ServiceTypeMismatch`].
pub struct ServiceKey<T> {
    name: Arc<str>,
    _type: PhantomData<fn() -> T>,
}

impl<T> Clone for ServiceKey<T> {
    fn clone(&self) -> Self {
        Self {
            name: Arc::clone(&self.name),
            _type: PhantomData,
        }
    }
}

impl<T> fmt::Debug for ServiceKey<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceKey")
            .field("name", &self.name)
            .finish()
    }
}

impl<T: Send + Sync + 'static> ServiceKey<T> {
    /// Creates a key for `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Arc::from(name.into().as_str()),
            _type: PhantomData,
        }
    }

    /// The service name carried by this key.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
}

/// A pinned lease on one published service binding.
///
/// The lease observes the binding it was issued for — a later replacement
/// of the provider is *not* followed (docs/04 §1.3: a generation must not
/// suddenly observe another provider). `snapshot` reads the binding's
/// current value, so it sees later `set` updates, and it is refused with
/// [`Error::ServiceRetired`] once the binding is retired.
///
/// Arcs already returned by [`snapshot`](Self::snapshot) are never
/// revoked or modified in place; retiring only blocks new reads.
pub struct ServiceLease<T> {
    binding: BindingId,
    cell: Arc<LeaseCell>,
    _type: PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> Clone for ServiceLease<T> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding,
            cell: Arc::clone(&self.cell),
            _type: PhantomData,
        }
    }
}

impl<T: Send + Sync + 'static> fmt::Debug for ServiceLease<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceLease")
            .field("binding", &self.binding)
            .field("revision", &self.revision())
            .finish()
    }
}

impl<T: Send + Sync + 'static> ServiceLease<T> {
    pub(crate) fn new(binding: BindingId, cell: Arc<LeaseCell>) -> Self {
        Self {
            binding,
            cell,
            _type: PhantomData,
        }
    }

    /// The binding this lease is pinned to.
    pub fn binding_id(&self) -> BindingId {
        self.binding
    }

    /// The value revision at the time of the last change, if live.
    ///
    /// The revision counts `set` operations on the binding; it is not the
    /// dependency epoch and never drives consumer reloads.
    pub fn revision(&self) -> Option<u64> {
        match &*self.cell.0.lock().expect("cordis lease cell lock") {
            CellValue::Live { revision, .. } => Some(*revision),
            CellValue::Retired => None,
        }
    }

    /// Reads the binding's current value.
    ///
    /// Returns [`Error::ServiceRetired`] after the binding retired. The
    /// downcast to `Arc<T>` cannot fail: the actor type-checked the
    /// binding before issuing this lease.
    pub fn snapshot(&self) -> Result<Arc<T>, Error> {
        let guard = self.cell.0.lock().expect("cordis lease cell lock");
        match &*guard {
            CellValue::Live { value, .. } => {
                Arc::clone(value)
                    .downcast::<T>()
                    .map_err(|_| Error::ServiceTypeMismatch {
                        service: String::new(),
                        namespace: String::new(),
                    })
            }
            CellValue::Retired => Err(Error::ServiceRetired {
                binding: self.binding,
            }),
        }
    }
}

/// Erased lease returned by [`lookup_dynamic`](crate::Context::lookup_dynamic).
///
/// Dynamic lookups query the live registry by name and carry **no
/// dependency-tracking guarantees**: they neither add the caller's
/// dependency nor trigger reloads, and the binding may be retired under
/// them at any time. Intended for management and diagnostics, or for
/// callers that explicitly accept dynamic behavior.
pub struct DynamicLease {
    binding: BindingId,
    cell: Arc<LeaseCell>,
    type_id: TypeId,
}

impl fmt::Debug for DynamicLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicLease")
            .field("binding", &self.binding)
            .finish()
    }
}

impl DynamicLease {
    pub(crate) fn new(binding: BindingId, cell: Arc<LeaseCell>, type_id: TypeId) -> Self {
        Self {
            binding,
            cell,
            type_id,
        }
    }

    /// The binding this handle observes.
    pub fn binding_id(&self) -> BindingId {
        self.binding
    }

    /// The payload type of the observed binding.
    pub fn value_type(&self) -> TypeId {
        self.type_id
    }

    /// Reads the binding's current value without a type conversion.
    ///
    /// Refused with [`Error::ServiceRetired`] once the binding retired.
    pub fn snapshot(&self) -> Result<Arc<dyn Any + Send + Sync>, Error> {
        let guard = self.cell.0.lock().expect("cordis lease cell lock");
        match &*guard {
            CellValue::Live { value, .. } => Ok(Arc::clone(value)),
            CellValue::Retired => Err(Error::ServiceRetired {
                binding: self.binding,
            }),
        }
    }

    /// Reads the binding's current value as `Arc<T>`.
    ///
    /// Fails with [`Error::ServiceTypeMismatch`] when `T` does not match
    /// the binding's payload type, and with [`Error::ServiceRetired`]
    /// after retirement.
    pub fn snapshot_as<T>(&self) -> Result<Arc<T>, Error>
    where
        T: Send + Sync + 'static,
    {
        if TypeId::of::<T>() != self.type_id {
            return Err(Error::ServiceTypeMismatch {
                service: String::new(),
                namespace: String::new(),
            });
        }
        self.snapshot()?
            .downcast::<T>()
            .map_err(|_| Error::ServiceTypeMismatch {
                service: String::new(),
                namespace: String::new(),
            })
    }
}

/// The shared value cell behind every lease (crate-internal).
///
/// Exactly one cell exists per binding. The actor mutates it on `set`
/// (value + revision) and on retirement (value extracted for off-actor
/// drop); leases hold `Arc` clones and read under the mutex.
pub(crate) struct LeaseCell(Mutex<CellValue>);

pub(crate) enum CellValue {
    Live { value: AnyConfig, revision: u64 },
    Retired,
}

impl LeaseCell {
    pub(crate) fn new(value: AnyConfig) -> Self {
        Self(Mutex::new(CellValue::Live { value, revision: 0 }))
    }

    /// Replaces the value, bumping the revision; returns the new revision.
    ///
    /// A retired cell never stores again: the registry removes the
    /// binding record in the same actor transition that retires the
    /// cell, so `set` reaching a retired cell is unreachable through the
    /// coordinator (callers resolve the binding record first).
    pub(crate) fn set(&self, value: AnyConfig) -> u64 {
        let mut guard = self.0.lock().expect("cordis lease cell lock");
        match &mut *guard {
            CellValue::Live { revision, .. } => {
                *revision += 1;
                let next = *revision;
                *guard = CellValue::Live {
                    value,
                    revision: next,
                };
                next
            }
            CellValue::Retired => 0,
        }
    }

    /// Retires the cell and extracts the value for off-actor drop.
    ///
    /// The last `Arc` of a user payload may run arbitrary `Drop` code; the
    /// actor must never drop it inline (docs/08-decisions.md D22), so the
    /// extracted value travels to the retirement lane.
    pub(crate) fn retire(&self) -> Option<AnyConfig> {
        let mut guard = self.0.lock().expect("cordis lease cell lock");
        match std::mem::replace(&mut *guard, CellValue::Retired) {
            CellValue::Live { value, .. } => Some(value),
            CellValue::Retired => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Namespace scopes
// ---------------------------------------------------------------------------

/// Allocates unique namespace nonces for `isolate` views.
static NEXT_SCOPE_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// The namespace one service slot lives in (docs/04 §1.1).
///
/// Structured on purpose: a shared label can never collide with a default
/// namespace or with an internal nonce, because the variants themselves
/// differ.
///
/// - [`ScopeId::Default`] is the app-global namespace every service name
///   implicitly resolves to;
/// - [`ScopeId::Unique`] is a fresh namespace created by
///   [`Context::isolate`](crate::Context::isolate) — lookups inside it
///   never fall back to outer namespaces;
/// - [`ScopeId::Shared`] is a named namespace that different views can
///   deliberately merge by using the same label.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ScopeId {
    /// The implicit per-service-name default namespace.
    Default,
    /// An isolated namespace; the number is only an identity nonce.
    Unique(u64),
    /// A shared, labeled namespace.
    Shared(String),
}

impl ScopeId {
    /// A stable rendering used in diagnostics and namespace hashes.
    pub fn as_str(&self) -> String {
        match self {
            ScopeId::Default => "default".to_owned(),
            ScopeId::Unique(nonce) => format!("unique({nonce})"),
            ScopeId::Shared(label) => format!("shared({label:?})"),
        }
    }
}

impl fmt::Display for ScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_str())
    }
}

impl fmt::Debug for ScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_str())
    }
}

/// A stable ordering key over `(discriminant, nonce/label)`.
pub(crate) fn scope_sort_key(scope: &ScopeId) -> (u8, u64, String) {
    match scope {
        ScopeId::Default => (0, 0, String::new()),
        ScopeId::Unique(nonce) => (1, *nonce, String::new()),
        ScopeId::Shared(label) => (2, 0, label.clone()),
    }
}

/// The immutable service-name → [`ScopeId`] chain carried by a
/// [`Context`](crate::Context) (docs/04 §1.1).
///
/// Resolution walks the chain from the innermost node outward and the
/// **nearest mapping wins**; a name nobody mapped resolves to
/// [`ScopeId::Default`]. Views are immutable: `fork` inherits the chain,
/// `isolate`/`isolate_shared` derive a new chain without mutating the
/// original view (docs/06 P4.6: parameter changes derive new views, they
/// never rewrite an active context).
#[derive(Clone)]
pub(crate) struct ScopeChain {
    node: Arc<ScopeNode>,
}

struct ScopeNode {
    mappings: HashMap<String, ScopeId>,
    parent: Option<Arc<ScopeNode>>,
}

impl fmt::Debug for ScopeChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Depth only: names/labels are service labels, not payloads, but
        // the chain itself is internal plumbing.
        let mut depth = 0;
        let mut node: Option<Arc<ScopeNode>> = Some(Arc::clone(&self.node));
        while let Some(current) = node {
            depth += 1;
            node = current.parent.clone();
        }
        write!(f, "ScopeChain(depth={depth})")
    }
}

impl ScopeChain {
    /// The empty chain every root context starts with.
    pub(crate) fn root() -> Self {
        Self {
            node: Arc::new(ScopeNode {
                mappings: HashMap::new(),
                parent: None,
            }),
        }
    }

    /// Derives a chain mapping `service` to a fresh unique namespace.
    pub(crate) fn isolate(&self, service: &str) -> Self {
        let nonce = NEXT_SCOPE_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.derive(service, ScopeId::Unique(nonce))
    }

    /// Derives a chain mapping `service` to the shared namespace `label`.
    pub(crate) fn isolate_shared(&self, service: &str, label: &str) -> Self {
        self.derive(service, ScopeId::Shared(label.to_owned()))
    }

    fn derive(&self, service: &str, scope: ScopeId) -> Self {
        let mut mappings = HashMap::with_capacity(1);
        mappings.insert(service.to_owned(), scope);
        Self {
            node: Arc::new(ScopeNode {
                mappings,
                parent: Some(Arc::clone(&self.node)),
            }),
        }
    }

    /// Resolves the namespace of `service`: nearest mapping wins, with
    /// [`ScopeId::Default`] as the implicit fallback.
    pub(crate) fn resolve(&self, service: &str) -> ScopeId {
        let mut node = Some(&*self.node);
        while let Some(current) = node {
            if let Some(scope) = current.mappings.get(service) {
                return scope.clone();
            }
            node = current.parent.as_deref();
        }
        ScopeId::Default
    }
}

// ---------------------------------------------------------------------------
// Dependency metadata
// ---------------------------------------------------------------------------

/// One dependency declaration on a plugin definition.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct RequiredDecl {
    pub(crate) name: String,
    pub(crate) type_id: TypeId,
}

/// A required dependency resolved against a concrete namespace slot at
/// load admission.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct RequiredSlot {
    pub(crate) name: String,
    pub(crate) scope: ScopeId,
    pub(crate) type_id: TypeId,
}

/// One entry of a generation's pinned dependency vector
/// (docs/04 §1.2: sorted `(BindingId, availability generation)` pairs).
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct DepRef {
    pub(crate) name: String,
    pub(crate) scope: ScopeId,
    pub(crate) binding: BindingId,
    pub(crate) availability_generation: u64,
}

/// The boxed start future of a managed service (runs on a supervised
/// worker; the binding publishes only after it succeeds).
pub(crate) type ManagedStartFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(), crate::error::PluginError>> + Send>,
>;

/// The boxed constructor of a [`ManagedStartFuture`].
pub(crate) type ManagedStartFn = Box<dyn FnOnce() -> ManagedStartFuture + Send>;

#[cfg(test)]
mod tests {
    use super::*;

    struct Db {
        url: String,
    }

    #[test]
    fn scope_chain_resolution_prefers_the_nearest_mapping() {
        let root = ScopeChain::root();
        assert_eq!(root.resolve("db"), ScopeId::Default);

        let outer = root.isolate_shared("db", "team-a");
        assert_eq!(outer.resolve("db"), ScopeId::Shared("team-a".to_owned()));
        // Unmapped services still resolve to their default namespace.
        assert_eq!(outer.resolve("cache"), ScopeId::Default);

        // An inner isolate shadows the outer shared mapping.
        let inner = outer.isolate("db");
        assert!(matches!(inner.resolve("db"), ScopeId::Unique(_)));

        // Views are immutable: deriving did not touch the parents.
        assert_eq!(outer.resolve("db"), ScopeId::Shared("team-a".to_owned()));
        assert_eq!(root.resolve("db"), ScopeId::Default);

        // fork inherits exactly the same chain.
        let fork = outer.clone();
        assert_eq!(fork.resolve("db"), outer.resolve("db"));
    }

    #[test]
    fn scope_ids_render_stable_namespace_strings() {
        assert_eq!(ScopeId::Default.as_str(), "default");
        assert_eq!(ScopeId::Unique(9).as_str(), "unique(9)");
        assert_eq!(ScopeId::Shared("x".to_owned()).as_str(), "shared(\"x\")");
        // Structured variants never compare equal across kinds, even with
        // identical renderings of their payloads.
        assert_ne!(ScopeId::Unique(0), ScopeId::Default);
    }

    #[test]
    fn lease_cell_set_bumps_revision_and_retire_extracts_value() {
        let cell = Arc::new(LeaseCell::new(Arc::new(Db {
            url: "a".to_owned(),
        })));
        assert_eq!(
            cell.set(Arc::new(Db {
                url: "b".to_owned()
            })),
            1
        );
        assert_eq!(
            cell.set(Arc::new(Db {
                url: "c".to_owned()
            })),
            2
        );
        let extracted = cell.retire().expect("value extracted");
        assert!(extracted.downcast::<Db>().is_ok());
        assert!(cell.retire().is_none(), "retire is once");
        assert!(
            matches!(&*cell.0.lock().unwrap(), CellValue::Retired),
            "cell is retired after extraction"
        );
    }

    #[test]
    fn typed_lease_snapshot_downcasts_and_rejects_after_retire() {
        let cell = Arc::new(LeaseCell::new(Arc::new(Db {
            url: "postgres".to_owned(),
        })));
        let binding = BindingId::new(
            crate::id::FiberId::alloc_global(),
            crate::id::GenerationId::alloc_global(),
            1,
        );
        let lease = ServiceLease::<Db>::new(binding, Arc::clone(&cell));
        assert_eq!(lease.snapshot().unwrap().url, "postgres");
        assert_eq!(lease.revision(), Some(0));

        cell.set(Arc::new(Db {
            url: "mysql".to_owned(),
        }));
        assert_eq!(lease.snapshot().unwrap().url, "mysql");
        assert_eq!(lease.revision(), Some(1));

        let taken = lease.snapshot().unwrap();
        cell.retire();
        assert!(matches!(
            lease.snapshot(),
            Err(Error::ServiceRetired { .. })
        ));
        assert_eq!(lease.revision(), None);
        // The previously taken arc is unaffected by retirement.
        assert_eq!(taken.url, "mysql");
    }

    #[test]
    fn dynamic_lease_type_checks() {
        let cell = Arc::new(LeaseCell::new(Arc::new(Db {
            url: "x".to_owned(),
        })));
        let lease = DynamicLease::new(
            BindingId::new(
                crate::id::FiberId::alloc_global(),
                crate::id::GenerationId::alloc_global(),
                2,
            ),
            Arc::clone(&cell),
            TypeId::of::<Db>(),
        );
        assert_eq!(lease.value_type(), TypeId::of::<Db>());
        assert_eq!(lease.snapshot_as::<Db>().unwrap().url, "x");
        assert!(matches!(
            lease.snapshot_as::<String>(),
            Err(Error::ServiceTypeMismatch { .. })
        ));
    }
}
