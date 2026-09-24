//! The plugin registry (docs/04 §4.2): names map to typed load closures.
//!
//! The core's `Plugin<C>` is typed; the registry erases the config type
//! behind a caller-supplied **decode closure**, so no reflection and no
//! serde requirement leaks into core. Hosts that derive serde types
//! register with [`Registry::register_serde`], decoding through
//! serde_json (serde stays inside the loader crate).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use cordis_core::{Context, FiberHandle, Plugin};
use serde_json::Value as Json;

use crate::error::{LoaderError, Result};

/// Boxed future shape used by the erased boundaries below.
pub(crate) type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The result one load produces.
pub(crate) type LoadResult = Result<Box<dyn MountedHandle>>;
/// A shared, cloneable erased handle.
pub(crate) type SharedHandle = Arc<dyn MountedHandle>;
/// The erased load entry point.
pub(crate) type LoadFn =
    Arc<dyn for<'a> Fn(&'a Context, &'a Json) -> BoxFut<'a, LoadResult> + Send + Sync>;
/// The decode-only probe.
pub(crate) type ProbeFn = Arc<dyn Fn(&Json) -> Result<()> + Send + Sync>;

/// The type-erased control surface of one mounted plugin entry: observe
/// the fiber, submit updates decoded through the registered closure,
/// and dispose it. All methods return boxed futures because the trait
/// object erases the config type that `FiberHandle<C>` carries.
pub(crate) trait MountedHandle: Send + Sync {
    fn fiber_id(&self) -> cordis_core::FiberId;
    fn dispose(&self) -> BoxFut<'_, Result<cordis_core::Operation>>;
    fn status(&self) -> BoxFut<'_, Result<cordis_core::FiberStatus>>;
    fn update_from<'a>(&'a self, config: &'a Json) -> BoxFut<'a, Result<cordis_core::Operation>>;
}

/// The per-registration typed decoder.
type DecodeFn<C> = Arc<dyn Fn(&Json) -> Result<C> + Send + Sync>;

struct TypedHandle<C> {
    handle: FiberHandle<C>,
    decode: DecodeFn<C>,
}

impl<C> MountedHandle for TypedHandle<C>
where
    C: Send + Sync + 'static,
{
    fn fiber_id(&self) -> cordis_core::FiberId {
        self.handle.fiber_id()
    }

    fn dispose(&self) -> BoxFut<'_, Result<cordis_core::Operation>> {
        Box::pin(async move { self.handle.dispose().await.map_err(runtime_error) })
    }

    fn update_from<'a>(&'a self, config: &'a Json) -> BoxFut<'a, Result<cordis_core::Operation>> {
        Box::pin(async move {
            let decoded = (self.decode)(config)?;
            self.handle.update(decoded).await.map_err(runtime_error)
        })
    }

    fn status(&self) -> BoxFut<'_, Result<cordis_core::FiberStatus>> {
        Box::pin(async move { self.handle.status().await.map_err(runtime_error) })
    }
}

/// What one registered name knows how to do: decode a JSON config and
/// load a fiber of the plugin under a context.
pub(crate) struct Registered {
    pub(crate) load: LoadFn,
    /// Runs only the decode half and drops the value: lets predecode
    /// prove a config decodes without a context or a runtime.
    pub(crate) probe: ProbeFn,
}

fn runtime_error(error: cordis_core::Error) -> LoaderError {
    LoaderError::MountFailed {
        reason: error.to_string(),
    }
}

/// Immutable snapshot of a registry, captured by group plugins and by
/// mount/reconcile runs: the load closures are `Arc`s, so snapshots are
/// cheap and stay consistent with what the host registered.
#[derive(Clone)]
pub(crate) struct RegistrySnapshot {
    entries: Vec<(String, RegisteredSnapshot)>,
}

#[derive(Clone)]
pub(crate) struct RegisteredSnapshot {
    pub(crate) load: LoadFn,
}

impl RegistrySnapshot {
    pub(crate) fn get(&self, name: &str) -> Option<&RegisteredSnapshot> {
        self.entries
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, registered)| registered)
    }
}

/// Registry of names to typed plugin constructors.
#[derive(Default)]
pub struct Registry {
    plugins: HashMap<String, Registered>,
}

impl Registry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `plugin` under `name` with a caller-supplied decode
    /// closure turning JSON configs into the plugin's typed config.
    ///
    /// A name registers once; duplicates are refused
    /// (docs/04 §4.2). The plugin's own `require` declarations carry its
    /// typed dependencies; loader-level inject lists are appended at
    /// compose time by prefixing them to the node's declared names —
    /// name-only dependencies resolve when the service registry knows
    /// the type (docs/04 §2).
    pub fn register<C>(
        &mut self,
        name: impl Into<String>,
        plugin: Plugin<C>,
        decode: impl Fn(&Json) -> Result<C> + Send + Sync + 'static,
    ) -> Result<()>
    where
        C: Send + Sync + 'static,
    {
        let name = name.into();
        if name.is_empty() {
            return Err(LoaderError::Parse {
                layer: "<registry>".to_owned(),
                reason: "plugin name must not be empty".to_owned(),
            });
        }
        if self.plugins.contains_key(&name) {
            return Err(LoaderError::DuplicateRegistration { plugin: name });
        }
        let decode = Arc::new(decode);
        let load_decode = Arc::clone(&decode);
        let probe_decode = Arc::clone(&decode);
        // The plugin is shared behind an `Arc` so the `Fn` load closure
        // can serve any number of loads.
        let plugin = Arc::new(plugin);
        self.plugins.insert(
            name,
            Registered {
                probe: Arc::new(move |config: &Json| probe_decode(config).map(|_| ())),
                load: Arc::new(move |ctx: &Context, config: &Json| {
                    let typed = match (load_decode)(config) {
                        Ok(value) => value,
                        Err(error) => return Box::pin(async move { Err(error) }),
                    };
                    let decode = Arc::clone(&load_decode);
                    let plugin = Arc::clone(&plugin);
                    Box::pin(async move {
                        let handle = ctx
                            .load(plugin.as_ref(), typed)
                            .await
                            .map_err(runtime_error)?;
                        Ok(Box::new(TypedHandle {
                            handle: handle.fiber,
                            decode,
                        }) as Box<dyn MountedHandle>)
                    })
                }),
            },
        );
        Ok(())
    }

    /// Registers a plugin whose config implements `DeserializeOwned`;
    /// decode goes through serde_json.
    pub fn register_serde<C>(&mut self, name: impl Into<String>, plugin: Plugin<C>) -> Result<()>
    where
        C: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        self.register(name, plugin, |config: &Json| {
            serde_json::from_value(config.clone()).map_err(|error| LoaderError::InvalidConfig {
                entry: String::new(),
                plugin: String::new(),
                reason: error.to_string(),
            })
        })
    }

    /// Whether a name is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.plugins.contains_key(name)
    }

    /// The registered names, sorted.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.plugins.keys().cloned().collect();
        names.sort();
        names
    }

    pub(crate) fn get(&self, name: &str) -> Option<&Registered> {
        self.plugins.get(name)
    }

    /// Builds the cheap `Arc`-backed snapshot group plugins capture.
    pub(crate) fn snapshot(&self) -> RegistrySnapshot {
        RegistrySnapshot {
            entries: self
                .plugins
                .iter()
                .map(|(name, registered)| {
                    (
                        name.clone(),
                        RegisteredSnapshot {
                            load: Arc::clone(&registered.load),
                        },
                    )
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cordis_core::define;

    struct Cfg {
        #[allow(dead_code)]
        value: u32,
    }

    #[test]
    fn duplicate_registration_is_refused() {
        let plugin: Plugin<Cfg> = define("p", |_ctx, _cfg: Arc<Cfg>| async { Ok(()) });
        let mut registry = Registry::new();
        registry
            .register("p", plugin.clone(), |_| Ok(Cfg { value: 0 }))
            .expect("first");
        let err = registry
            .register("p", plugin, |_| Ok(Cfg { value: 1 }))
            .expect_err("second");
        assert!(matches!(err, LoaderError::DuplicateRegistration { .. }));
        assert_eq!(registry.names(), vec!["p"]);
    }
}
