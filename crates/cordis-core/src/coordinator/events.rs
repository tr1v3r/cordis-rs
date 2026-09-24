//! The event registry owned by the coordinator actor
//! (docs/04-services-events-loader.md §3.2–§3.3).
//!
//! Separate from the service registry: identical names never contend for
//! slots across the two systems. The registry keeps, per event name:
//!
//! - the fixed **mode** and payload/response `TypeId`s — every later
//!   registration or dispatch is checked against them, so a name can
//!   never mix modes or types (V34, docs/04 §3.1);
//! - the ordered listener list (prepend inserts at the head); listeners
//!   registered by a `Starting` generation are **staged** and only join
//!   the selection once their generation commits;
//! - the per-listener records: owning effect entry, namespace, flags and
//!   the shared erased handler.
//!
//! Dispatch selection and admission live in the coordinator's dispatch
//! handler: the snapshot and the per-listener admission claim happen in
//! one serial transition, so a listener disposed after the snapshot can
//! never run, and a `once` listener is claimed by at most one of any
//! number of racing dispatches (V32/V33).

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Error;
use crate::events::{EventMode, ListenerConfig, ListenerHandler};
use crate::id::EffectId;
use crate::services::ScopeId;

/// One registered listener. Keyed by its owning effect entry, whose
/// lifetime is the listener's lifetime.
pub(crate) struct ListenerRec {
    pub(crate) event: String,
    /// Namespace the listener registered under (docs/04 §3.3).
    pub(crate) scope: ScopeId,
    pub(crate) global: bool,
    pub(crate) once: bool,
    /// Registered by a Starting generation; excluded from selection until
    /// the generation commits (docs/04 §3.2).
    pub(crate) staged: bool,
    /// A once listener already claimed by an admitted dispatch.
    pub(crate) claimed: bool,
    pub(crate) handler: Arc<ListenerHandler>,
}

/// The per-name event record: fixed identity plus ordered listeners.
pub(crate) struct EventRecord {
    pub(crate) mode: EventMode,
    pub(crate) payload_type: TypeId,
    pub(crate) response_type: Option<TypeId>,
    /// Dispatch order; prepend inserts at the head.
    pub(crate) listeners: Vec<EffectId>,
}

/// The event registry. Actor-confined like the service registry.
#[derive(Default)]
pub(crate) struct EventRegistry {
    events: HashMap<String, EventRecord>,
    listeners: HashMap<EffectId, ListenerRec>,
}

impl EventRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Number of live listeners (diagnostics).
    pub(crate) fn listeners_live(&self) -> usize {
        self.listeners.len()
    }

    /// Looks up an event record by name.
    pub(crate) fn event(&self, name: &str) -> Option<&EventRecord> {
        self.events.get(name)
    }

    /// Looks up a mutable event record by name.
    pub(crate) fn event_mut(&mut self, name: &str) -> Option<&mut EventRecord> {
        self.events.get_mut(name)
    }

    /// Looks up a listener by its owning effect entry.
    pub(crate) fn listener(&self, effect: &EffectId) -> Option<&ListenerRec> {
        self.listeners.get(effect)
    }

    /// Mutable listener lookup.
    pub(crate) fn listener_mut(&mut self, effect: &EffectId) -> Option<&mut ListenerRec> {
        self.listeners.get_mut(effect)
    }

    /// Registers a listener after validating the name's fixed identity
    /// (docs/04 §3.1): a mismatching mode, payload type or response type
    /// is rejected at registration time, never at call time.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn subscribe(
        &mut self,
        name: &str,
        mode: EventMode,
        payload_type: TypeId,
        response_type: Option<TypeId>,
        effect: EffectId,
        scope: ScopeId,
        config: ListenerConfig,
        staged: bool,
        handler: Arc<ListenerHandler>,
    ) -> Result<(), Error> {
        match self.events.get(name) {
            None => {
                self.events.insert(
                    name.to_owned(),
                    EventRecord {
                        mode,
                        payload_type,
                        response_type,
                        listeners: Vec::new(),
                    },
                );
            }
            Some(record) => {
                if record.mode != mode {
                    return Err(Error::EventConflict {
                        event: name.to_owned(),
                        reason: format!(
                            "event is registered in {} mode; {} registration rejected",
                            record.mode.as_str(),
                            mode.as_str()
                        ),
                    });
                }
                if record.payload_type != payload_type {
                    return Err(Error::EventConflict {
                        event: name.to_owned(),
                        reason: "event is registered with a different payload type".to_owned(),
                    });
                }
                if record.response_type != response_type {
                    return Err(Error::EventConflict {
                        event: name.to_owned(),
                        reason: "event is registered with a different response type".to_owned(),
                    });
                }
            }
        }
        let record = self.events.get_mut(name).expect("just ensured");
        if config.prepend {
            record.listeners.insert(0, effect);
        } else {
            record.listeners.push(effect);
        }
        self.listeners.insert(
            effect,
            ListenerRec {
                event: name.to_owned(),
                scope,
                global: config.global,
                once: config.once,
                staged,
                claimed: false,
                handler,
            },
        );
        Ok(())
    }

    /// Removes a listener (teardown of its entry, or completion of a
    /// once invocation). Returns the event name it belonged to.
    pub(crate) fn unsubscribe(&mut self, effect: &EffectId) -> Option<String> {
        let rec = self.listeners.remove(effect)?;
        if let Some(record) = self.events.get_mut(&rec.event) {
            record.listeners.retain(|id| id != effect);
        }
        Some(rec.event)
    }

    /// Unstages the listeners of one committed generation: they join the
    /// selection from now on (docs/04 §3.2).
    pub(crate) fn unstage_generation(&mut self, owned: &[EffectId]) -> usize {
        let mut unstaged = 0;
        for effect in owned {
            if let Some(rec) = self.listeners.get_mut(effect) {
                if rec.staged {
                    rec.staged = false;
                    unstaged += 1;
                }
            }
        }
        unstaged
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EmitFn, EventMode};
    use crate::services::ScopeId;
    use std::any::Any;

    fn emit_handler() -> Arc<ListenerHandler> {
        Arc::new(ListenerHandler::Emit(
            Box::new(|_payload: &(dyn Any + Send + Sync)| Ok(())) as EmitFn,
        ))
    }

    #[test]
    fn first_registration_fixes_the_identity() {
        let mut registry = EventRegistry::new();
        let effect = EffectId::alloc_global();
        registry
            .subscribe(
                "ping",
                EventMode::Emit,
                TypeId::of::<u32>(),
                None,
                effect,
                ScopeId::Default,
                ListenerConfig::default(),
                false,
                emit_handler(),
            )
            .expect("first registration fixes identity");

        // Same name, different mode: rejected at registration (V34).
        let conflict = registry.subscribe(
            "ping",
            EventMode::Bail,
            TypeId::of::<u32>(),
            Some(TypeId::of::<String>()),
            EffectId::alloc_global(),
            ScopeId::Default,
            ListenerConfig::default(),
            false,
            emit_handler(),
        );
        assert!(matches!(conflict, Err(Error::EventConflict { .. })));

        // Same name, different payload type: rejected.
        let conflict = registry.subscribe(
            "ping",
            EventMode::Emit,
            TypeId::of::<String>(),
            None,
            EffectId::alloc_global(),
            ScopeId::Default,
            ListenerConfig::default(),
            false,
            emit_handler(),
        );
        assert!(matches!(conflict, Err(Error::EventConflict { .. })));

        // Identical identity: admitted and ordered.
        let second = EffectId::alloc_global();
        registry
            .subscribe(
                "ping",
                EventMode::Emit,
                TypeId::of::<u32>(),
                None,
                second,
                ScopeId::Default,
                ListenerConfig::default(),
                false,
                emit_handler(),
            )
            .expect("identical registration admitted");
        assert_eq!(
            registry.event("ping").unwrap().listeners,
            vec![effect, second]
        );
    }

    #[test]
    fn prepend_inserts_at_the_head_and_unsubscribe_removes() {
        let mut registry = EventRegistry::new();
        let base = EffectId::alloc_global();
        let prepended = EffectId::alloc_global();
        registry
            .subscribe(
                "ping",
                EventMode::Emit,
                TypeId::of::<u32>(),
                None,
                base,
                ScopeId::Default,
                ListenerConfig::default(),
                false,
                emit_handler(),
            )
            .unwrap();
        registry
            .subscribe(
                "ping",
                EventMode::Emit,
                TypeId::of::<u32>(),
                None,
                prepended,
                ScopeId::Default,
                ListenerConfig::default().with_prepend(true),
                false,
                emit_handler(),
            )
            .unwrap();
        assert_eq!(
            registry.event("ping").unwrap().listeners,
            vec![prepended, base]
        );

        assert_eq!(registry.unsubscribe(&prepended).as_deref(), Some("ping"));
        assert_eq!(registry.event("ping").unwrap().listeners, vec![base]);
        assert!(registry.listener(&prepended).is_none());
    }
}
