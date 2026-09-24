//! The typed plugin definition and its internal erased representation.

use std::any::{Any, TypeId};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use crate::context::Context;
use crate::error::PluginError;
use crate::id::DefinitionId;

/// Type-erased configuration value stored per fiber.
pub(crate) type AnyConfig = Arc<dyn Any + Send + Sync>;

/// Owned future returned by an erased plugin activation.
// Driven by the coordinator from P2 on; until then only unit tests
// exercise the activation path.
#[allow(dead_code)]
pub(crate) type PluginFuture = Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>>;

/// Immutable metadata of a definition: identity plus diagnostic name.
#[derive(Debug, Clone)]
pub(crate) struct PluginMeta {
    pub(crate) name: String,
    pub(crate) definition_id: DefinitionId,
}

/// Private erased plugin boundary (docs/02-api.md §4).
///
/// Methods carry no type parameters and return owned boxed futures, so the
/// kernel can hold plugins of different config types uniformly. The public
/// side stays fully typed through [`Plugin`]; callers cannot construct an
/// unchecked erased plugin themselves.
// The activation half of this boundary is driven by the coordinator from
// P2 on; until then it is exercised only by unit tests.
#[allow(dead_code)]
pub(crate) trait ErasedPlugin: Send + Sync {
    fn meta(&self) -> &PluginMeta;
    fn config_type(&self) -> TypeId;
    fn activate(self: Arc<Self>, ctx: Context, config: AnyConfig) -> PluginFuture;
}

/// A typed, immutable plugin definition (docs/02-api.md §2).
///
/// Built once by [`define`]; every method is read-only. Cloning shares the
/// same [`DefinitionId`](crate::DefinitionId) — the clone *is* the same
/// definition — while a second `define` call creates an independent
/// definition even when the name and config type are identical (V02).
///
/// Loading the same definition repeatedly yields one shared runtime with
/// one fiber per load (V01); see
/// [`Context::load`](crate::Context::load).
pub struct Plugin<C> {
    erased: Arc<dyn ErasedPlugin>,
    _config: PhantomData<fn() -> C>,
}

impl<C> Clone for Plugin<C> {
    /// Clones share the definition identity: `clone().definition_id() ==
    /// self.definition_id()`.
    fn clone(&self) -> Self {
        Self {
            erased: Arc::clone(&self.erased),
            _config: PhantomData,
        }
    }
}

impl<C> fmt::Debug for Plugin<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let meta = self.erased.meta();
        f.debug_struct("Plugin")
            .field("name", &meta.name)
            .field("definition", &meta.definition_id)
            .finish()
    }
}

impl<C> Plugin<C> {
    /// Returns the diagnostic name given to [`define`].
    ///
    /// The name is a label chosen by the definer, not configuration data.
    pub fn name(&self) -> &str {
        &self.erased.meta().name
    }

    /// Returns the identity allocated when this definition was created.
    pub fn definition_id(&self) -> DefinitionId {
        self.erased.meta().definition_id
    }

    /// Returns the erased definition behind the typed facade.
    ///
    /// Crate-internal: the coordinator captures this to spawn activation
    /// workers; the public surface stays typed.
    pub(crate) fn erased_clone(&self) -> Arc<dyn ErasedPlugin> {
        Arc::clone(&self.erased)
    }
}

/// Defines a plugin: an immutable definition identified by a fresh
/// [`DefinitionId`](crate::DefinitionId).
///
/// `apply` receives the [`Context`] of the fiber's current generation and
/// the fiber's immutable configuration as `Arc<C>`. It runs on a supervised
/// activation worker (never inside the coordinator) once the lifecycle
/// exists (P2); at P1 the definition is registered and the closure is kept
/// for the future coordinator.
///
/// Two `define` calls never share identity, even with identical names and
/// config types (V02).
pub fn define<C, F, Fut>(name: impl Into<String>, apply: F) -> Plugin<C>
where
    C: Send + Sync + 'static,
    F: Fn(Context, Arc<C>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), PluginError>> + Send + 'static,
{
    let erased: Arc<dyn ErasedPlugin> = Arc::new(TypedPlugin {
        meta: PluginMeta {
            name: name.into(),
            definition_id: DefinitionId::alloc_global(),
        },
        apply,
        _config: PhantomData,
    });
    Plugin {
        erased,
        _config: PhantomData,
    }
}

/// Generic adapter implementing [`ErasedPlugin`] for a typed closure.
struct TypedPlugin<C, F> {
    meta: PluginMeta,
    // Read only inside `activate`, which the P2 coordinator drives; unit
    // tests exercise the path until then.
    #[allow(dead_code)]
    apply: F,
    _config: PhantomData<fn() -> C>,
}

impl<C, F, Fut> ErasedPlugin for TypedPlugin<C, F>
where
    C: Send + Sync + 'static,
    F: Fn(Context, Arc<C>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), PluginError>> + Send + 'static,
{
    fn meta(&self) -> &PluginMeta {
        &self.meta
    }

    fn config_type(&self) -> TypeId {
        TypeId::of::<C>()
    }

    fn activate(self: Arc<Self>, ctx: Context, config: AnyConfig) -> PluginFuture {
        let this = self;
        match config.downcast::<C>() {
            // The future owns the config and the context: no borrows cross
            // an await boundary (docs/02-api.md §4).
            Ok(config) => Box::pin(async move { (this.apply)(ctx, config).await }),
            Err(_) => Box::pin(async {
                Err(PluginError::from(
                    "config type mismatch at the erased plugin boundary",
                ))
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Config {
        value: u32,
    }

    #[test]
    fn clone_shares_definition_identity() {
        let first = define("metrics", |_ctx, _cfg: Arc<Config>| async { Ok(()) });
        let second = first.clone();
        assert_eq!(first.definition_id(), second.definition_id());
        assert_eq!(first.name(), second.name());
        assert_eq!(format!("{first:?}"), format!("{second:?}"));
    }

    #[test]
    fn same_name_definitions_stay_independent() {
        let first = define("metrics", |_ctx, _cfg: Arc<Config>| async { Ok(()) });
        let second = define("metrics", |_ctx, _cfg: Arc<Config>| async { Ok(()) });
        assert_eq!(first.name(), second.name());
        assert_ne!(first.definition_id(), second.definition_id());
    }

    #[tokio::test]
    async fn erased_boundary_type_checks_and_activates() {
        let app = App::builder().build().unwrap();
        let activations = Arc::new(AtomicUsize::new(0));
        let seen_values = Arc::new(Mutex::new(Vec::new()));

        let counted = Arc::clone(&activations);
        let recorded = Arc::clone(&seen_values);
        let plugin = define("demo", move |_ctx, cfg: Arc<Config>| {
            let counted = Arc::clone(&counted);
            let recorded = Arc::clone(&recorded);
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                recorded.lock().unwrap().push(cfg.value);
                Ok(())
            }
        });

        // The erased side reports the typed config type.
        assert_eq!(plugin.erased.config_type(), TypeId::of::<Config>());

        // Correct type: the typed apply runs and observes the config.
        let ctx = app.context();
        let outcome = Arc::clone(&plugin.erased)
            .activate(ctx, Arc::new(Config { value: 7 }))
            .await;
        assert!(outcome.is_ok());

        // Wrong type: rejected in a controlled way, apply never runs (the
        // runtime half of V40).
        let ctx = app.context();
        let wrong_type: AnyConfig = Arc::new(String::from("not a Config"));
        let rejected = Arc::clone(&plugin.erased).activate(ctx, wrong_type).await;
        assert!(rejected.is_err());

        assert_eq!(activations.load(Ordering::SeqCst), 1);
        assert_eq!(*seen_values.lock().unwrap(), vec![7u32]);
    }

    #[test]
    fn public_handles_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Plugin<Config>>();
        assert_send_sync::<crate::FiberHandle<Config>>();
        assert_send_sync::<crate::Context>();
        assert_send_sync::<crate::App>();
        assert_send_sync::<crate::WeakApp>();
    }
}
