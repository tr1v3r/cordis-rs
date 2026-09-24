//! The P5 typed event system: keys, dispatch modes and middleware
//! (docs/04-services-events-loader.md §3).
//!
//! Three key kinds carry both a name and the expected payload/response
//! types; a fourth "mode" dimension (emit / bail / serial / parallel /
//! waterfall) is fixed per event **name** at first registration and every
//! later registration or dispatch is checked against it — a name can
//! never silently mix modes or types (V34).
//!
//! - [`EventKey<E>`] dispatches with **emit** semantics: sync handlers
//!   run in registration order, errors are collected and the caller
//!   observes a [`DispatchReport`].
//! - [`QueryKey<E, R>`] carries the two request/response modes with typed
//!   [`ControlFlow`] and one aggregate mode: **bail** (sync, short-circuit
//!   on `Break(R)` or `Err`), **serial** (async, awaited in order, same
//!   control flow) and **parallel** (async, bounded concurrency, results
//!   assembled in registration order).
//! - [`WaterfallKey<E, R>`] dispatches **around** middleware: each
//!   middleware receives the payload plus a move-only [`Next`] that may
//!   forward (possibly after modifying the payload), wrap the inner
//!   result, or short-circuit by not calling `next` at all. The
//!   dispatch-supplied final runs at most once.
//!
//! Listeners are effect entries: owned by the registering scope, staged
//! until the owning generation commits (or immediate for Active/root
//! owners), unsubscribed at teardown, and admitted per-dispatch so a
//! listener disposed after the snapshot never runs (V33).
//!
//! ## Minimal usage
//!
//! ```
//! use cordis_core::{App, EventKey, ListenerConfig, WaterfallKey, define};
//! use std::sync::{Arc, Mutex};
//!
//! struct Cfg;
//! struct Ping {
//!     seq: u32,
//! }
//! struct Greet {
//!     name: String,
//! }
//!
//! # fn main() {
//! let rt = tokio::runtime::Builder::new_current_thread()
//!     .enable_all()
//!     .build()
//!     .expect("runtime builds");
//! rt.block_on(async {
//!     let app = App::builder().build().expect("app builds");
//!     let root = app.context();
//!     let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
//!
//!     let plugin = {
//!         let log = Arc::clone(&log);
//!         define("bus", move |ctx, _cfg: Arc<Cfg>| {
//!             let log = Arc::clone(&log);
//!             let ping = EventKey::<Ping>::new("ping");
//!             let greet = WaterfallKey::<Greet, String>::new("greet");
//!             async move {
//!                 let emit_log = Arc::clone(&log);
//!                 ctx.on_emit(
//!                     ping,
//!                     move |event: &Ping| {
//!                         emit_log.lock().unwrap().push(format!("ping {}", event.seq));
//!                         Ok(())
//!                     },
//!                     ListenerConfig::default(),
//!                 )
//!                 .await?;
//!                 ctx.on_waterfall(
//!                     greet,
//!                     |mut event: Greet, next| async move {
//!                         // Middleware may modify the payload on the way
//!                         // down and wrap the result on the way up.
//!                         event.name.push('!');
//!                         let inner = next.run(event).await?;
//!                         Ok(format!("hello {inner}"))
//!                     },
//!                     ListenerConfig::default(),
//!                 )
//!                 .await?;
//!                 Ok(())
//!             }
//!         })
//!     };
//!
//!     let receipt = root.load(&plugin, Cfg).await.expect("load admitted");
//!     receipt.operation.wait().await.expect("active");
//!
//!     let report = root
//!         .emit(EventKey::<Ping>::new("ping"), Ping { seq: 1 })
//!         .await
//!         .expect("emit dispatch");
//!     assert!(report.is_clean());
//!     assert_eq!(*log.lock().unwrap(), vec!["ping 1".to_owned()]);
//!
//!     let greeting = root
//!         .waterfall(
//!             WaterfallKey::<Greet, String>::new("greet"),
//!             Greet { name: "cordis".into() },
//!             |event| async move { Ok(event.name) },
//!         )
//!         .await
//!         .expect("waterfall dispatch");
//!     assert_eq!(greeting, "hello cordis!");
//! });
//! # }
//! ```

use std::any::Any;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::{Error, PluginError};
use crate::id::EffectId;

/// The dispatch mode fixed per event name at first registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum EventMode {
    /// Sync handlers, errors collected into a [`DispatchReport`].
    Emit,
    /// Sync handlers with typed control flow, short-circuit on `Break`.
    Bail,
    /// Async handlers awaited in order with typed control flow.
    Serial,
    /// Async handlers under bounded concurrency, aggregated in order.
    Parallel,
    /// Async around-middleware with a move-only `Next`.
    Waterfall,
}

impl EventMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            EventMode::Emit => "emit",
            EventMode::Bail => "bail",
            EventMode::Serial => "serial",
            EventMode::Parallel => "parallel",
            EventMode::Waterfall => "waterfall",
        }
    }
}

/// Key of a plain event dispatched with **emit** semantics.
pub struct EventKey<E> {
    name: Arc<str>,
    _payload: PhantomData<fn() -> E>,
}

impl<E> Clone for EventKey<E> {
    fn clone(&self) -> Self {
        Self {
            name: Arc::clone(&self.name),
            _payload: PhantomData,
        }
    }
}

impl<E> std::fmt::Debug for EventKey<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventKey")
            .field("name", &self.name)
            .finish()
    }
}

impl<E: Send + Sync + 'static> EventKey<E> {
    /// Creates a key for `name`.
    ///
    /// The payload bound is enforced here already: constructing a key for
    /// a `!Send`/`!Sync` payload does not compile (V40).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Arc::from(name.into().as_str()),
            _payload: PhantomData,
        }
    }

    /// The event name carried by this key.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Key of a request/response event dispatched with **bail**, **serial**
/// or **parallel** semantics.
pub struct QueryKey<E, R> {
    name: Arc<str>,
    _types: PhantomData<fn() -> (E, R)>,
}

impl<E, R> Clone for QueryKey<E, R> {
    fn clone(&self) -> Self {
        Self {
            name: Arc::clone(&self.name),
            _types: PhantomData,
        }
    }
}

impl<E, R> std::fmt::Debug for QueryKey<E, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryKey")
            .field("name", &self.name)
            .finish()
    }
}

impl<E: Send + Sync + 'static, R: Send + 'static> QueryKey<E, R> {
    /// Creates a key for `name`.
    ///
    /// Payload and response bounds are enforced at construction (V40).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Arc::from(name.into().as_str()),
            _types: PhantomData,
        }
    }

    /// The event name carried by this key.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Key of a middleware chain dispatched with **waterfall** semantics.
pub struct WaterfallKey<E, R> {
    name: Arc<str>,
    _types: PhantomData<fn() -> (E, R)>,
}

impl<E, R> Clone for WaterfallKey<E, R> {
    fn clone(&self) -> Self {
        Self {
            name: Arc::clone(&self.name),
            _types: PhantomData,
        }
    }
}

impl<E, R> std::fmt::Debug for WaterfallKey<E, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaterfallKey")
            .field("name", &self.name)
            .finish()
    }
}

impl<E: Send + Sync + 'static, R: Send + 'static> WaterfallKey<E, R> {
    /// Creates a key for `name`.
    ///
    /// Payload and response bounds are enforced at construction (V40).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Arc::from(name.into().as_str()),
            _types: PhantomData,
        }
    }

    /// The event name carried by this key.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Options of one listener registration.
///
/// The default registers a plain, ordered, non-once listener.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListenerConfig {
    /// Bypass scoped dispatch filtering: a global listener is selected
    /// even by scoped dispatches of other namespaces (docs/04 §3.3).
    pub global: bool,
    /// Insert at the head of the dispatch order instead of the tail.
    pub prepend: bool,
    /// Run for exactly one admitted invocation; the listener and its
    /// effect entry retire after the handler settles.
    pub once: bool,
}

impl ListenerConfig {
    /// A listener that ignores scoped filtering.
    pub fn global() -> Self {
        Self {
            global: true,
            ..Self::default()
        }
    }

    /// A once-listener inserted at the head of the order.
    pub fn once() -> Self {
        Self {
            once: true,
            ..Self::default()
        }
    }

    /// Sets the prepend flag.
    pub fn with_prepend(mut self, prepend: bool) -> Self {
        self.prepend = prepend;
        self
    }

    /// Sets the global flag.
    pub fn with_global(mut self, global: bool) -> Self {
        self.global = global;
        self
    }

    /// Sets the once flag.
    pub fn with_once(mut self, once: bool) -> Self {
        self.once = once;
        self
    }
}

/// Aggregate report of one **emit** dispatch.
///
/// Every selected listener ran: errors are collected per listener instead
/// of short-circuiting the rest (docs/04 §3.1).
#[derive(Debug, Default)]
pub struct DispatchReport {
    /// Handlers that completed without an error.
    pub delivered: usize,
    /// Per-listener failures, in dispatch order.
    pub failures: Vec<DispatchFailure>,
}

impl DispatchReport {
    /// `true` when every selected handler succeeded.
    pub fn is_clean(&self) -> bool {
        self.failures.is_empty()
    }
}

/// One failed listener inside a [`DispatchReport`].
#[derive(Debug)]
pub struct DispatchFailure {
    /// The listener (effect entry) whose handler failed.
    pub listener: EffectId,
    /// The error the handler returned, or the rendered panic.
    pub error: PluginError,
}

/// Per-handler results of one **parallel** dispatch, in registration
/// order regardless of completion order (docs/04 §3.1).
#[derive(Debug)]
pub struct ParallelReport<R> {
    /// One entry per admitted handler, in dispatch order.
    pub results: Vec<Result<R, PluginError>>,
}

impl<R> ParallelReport<R> {
    /// `true` when every admitted handler succeeded.
    pub fn is_clean(&self) -> bool {
        self.results.iter().all(|entry| entry.is_ok())
    }
}

impl std::fmt::Debug for DispatchOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never prints payloads or responses.
        match self {
            DispatchOutcome::Report(report) => f
                .debug_tuple("Report")
                .field(&report.delivered)
                .field(&report.failures.len())
                .finish(),
            DispatchOutcome::Flow(value) => f.debug_tuple("Flow").field(&value.is_some()).finish(),
            DispatchOutcome::Parallel(results) => {
                f.debug_tuple("Parallel").field(&results.len()).finish()
            }
            DispatchOutcome::Waterfall(_) => f.write_str("Waterfall(..)"),
        }
    }
}

// ---------------------------------------------------------------------------
// Erased handler and middleware representations (crate-internal)
// ---------------------------------------------------------------------------

/// Erased payload traveling through dispatches and waterfalls. Sync is
/// part of the erased surface because async modes share `Arc<E>` across
/// concurrently running handlers.
pub(crate) type EventPayload = Box<dyn Any + Send + Sync>;
/// Erased response of bail/serial/parallel/waterfall dispatches.
pub(crate) type EventResponse = Box<dyn Any + Send>;
/// Erased shared payload handed to async handlers (`Arc<E>`).
pub(crate) type SharedPayload = Arc<dyn Any + Send + Sync>;

/// The boxed sync handler of an emit listener.
pub(crate) type EmitFn =
    Box<dyn Fn(&(dyn Any + Send + Sync)) -> Result<(), PluginError> + Send + Sync>;
/// The boxed sync handler of a bail listener.
pub(crate) type BailFn = Box<
    dyn Fn(&(dyn Any + Send + Sync)) -> Result<std::ops::ControlFlow<EventResponse>, PluginError>
        + Send
        + Sync,
>;
/// The boxed async handler factory of a serial listener.
pub(crate) type SerialFn = Box<
    dyn Fn(
            SharedPayload,
        ) -> Pin<Box<dyn Future<Output = Result<ControlFlowErased, PluginError>> + Send>>
        + Send
        + Sync,
>;
/// The boxed async handler factory of a parallel listener.
pub(crate) type ParallelFn = Box<
    dyn Fn(
            SharedPayload,
        ) -> Pin<Box<dyn Future<Output = Result<EventResponse, PluginError>> + Send>>
        + Send
        + Sync,
>;
/// The boxed middleware of a waterfall listener.
pub(crate) type WaterfallFn = Box<
    dyn Fn(
            EventPayload,
            NextErased,
        ) -> Pin<Box<dyn Future<Output = Result<EventResponse, PluginError>> + Send>>
        + Send
        + Sync,
>;
/// The dispatch-supplied final of a waterfall.
pub(crate) type WaterfallFinal = Box<
    dyn FnOnce(
            EventPayload,
        ) -> Pin<Box<dyn Future<Output = Result<EventResponse, PluginError>> + Send>>
        + Send,
>;

/// The typed control flow of bail/serial handlers, with the break value
/// erased.
pub(crate) type ControlFlowErased = std::ops::ControlFlow<EventResponse>;

/// One listener's erased handler, shared between concurrent dispatches.
pub(crate) enum ListenerHandler {
    Emit(EmitFn),
    Bail(BailFn),
    Serial(SerialFn),
    Parallel(ParallelFn),
    Waterfall(WaterfallFn),
}

impl std::fmt::Debug for ListenerHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ListenerHandler(..)")
    }
}

/// How a dispatch carries its payload (docs/04 §3.1): sync modes borrow
/// an owned box, async modes share an arc per handler.
pub(crate) enum DispatchPayload {
    /// emit / bail / waterfall.
    Owned(EventPayload),
    /// serial / parallel.
    Shared(SharedPayload),
}

/// The erased completion value of one dispatch.
pub(crate) enum DispatchOutcome {
    /// emit: per-listener aggregation.
    Report(DispatchReport),
    /// bail / serial: the `Break` value, or `None` when every handler
    /// continued.
    Flow(Option<EventResponse>),
    /// parallel: per-handler results in dispatch order.
    Parallel(Vec<Result<EventResponse, PluginError>>),
    /// waterfall: the chain's response.
    Waterfall(EventResponse),
}

/// The remaining waterfall chain behind one [`Next`].
///
/// `run` consumes `self`, so a middleware can forward at most once; the
/// public typed [`Next`] inherits the move-only discipline and calling it
/// twice is a compile error (V31 compile-fail).
pub(crate) struct NextErased {
    chain: Arc<[Arc<ListenerHandler>]>,
    next_index: usize,
    final_: Option<Arc<OnceFinal>>,
}

/// Shared wrapper guaranteeing the final runs at most once even if a
/// middleware misbehaves across awaits (defense in depth; the type system
/// already prevents double-forwarding).
struct OnceFinal {
    used: std::sync::atomic::AtomicBool,
    inner: std::sync::Mutex<Option<WaterfallFinal>>,
}

impl NextErased {
    /// Builds the head `Next` of a waterfall dispatch.
    pub(crate) fn new(chain: Arc<[Arc<ListenerHandler>]>, final_: WaterfallFinal) -> Self {
        Self {
            chain,
            next_index: 0,
            final_: Some(Arc::new(OnceFinal {
                used: std::sync::atomic::AtomicBool::new(false),
                inner: std::sync::Mutex::new(Some(final_)),
            })),
        }
    }

    /// Runs the rest of the chain: the next middleware, or the final.
    pub(crate) async fn run(self, payload: EventPayload) -> Result<EventResponse, PluginError> {
        if self.next_index < self.chain.len() {
            let middleware = Arc::clone(&self.chain[self.next_index]);
            let rest = NextErased {
                chain: self.chain,
                next_index: self.next_index + 1,
                final_: self.final_,
            };
            let ListenerHandler::Waterfall(handler) = &*middleware else {
                return Err(PluginError::from(
                    "waterfall chain corrupted: non-middleware listener",
                ));
            };
            return handler(payload, rest).await;
        }
        let Some(final_) = self.final_ else {
            return Err(PluginError::from("waterfall final already consumed"));
        };
        if final_.used.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Err(PluginError::from("waterfall final ran twice"));
        }
        let taken = final_
            .inner
            .lock()
            .expect("cordis waterfall final lock")
            .take();
        match taken {
            Some(final_) => final_(payload).await,
            None => Err(PluginError::from("waterfall final already consumed")),
        }
    }
}

/// The typed, move-only continuation handed to a waterfall middleware.
///
/// `run` takes `self` by value: forwarding twice does not compile
/// (V31 compile-fail), and skipping the call short-circuits the chain.
pub struct Next<E, R> {
    erased: NextErased,
    _types: PhantomData<fn() -> (E, R)>,
}

impl<E: Send + Sync + 'static, R: Send + 'static> Next<E, R> {
    pub(crate) fn new(erased: NextErased) -> Self {
        Self {
            erased,
            _types: PhantomData,
        }
    }

    /// Forwards (possibly after the middleware modified the payload) and
    /// returns the inner chain's response, with the inner result typed
    /// back to `R`.
    pub async fn run(self, event: E) -> Result<R, Error> {
        let response = self
            .erased
            .run(Box::new(event) as EventPayload)
            .await
            .map_err(|source| Error::HandlerFailed {
                listener: None,
                source,
            })?;
        match response.downcast::<R>() {
            Ok(value) => Ok(*value),
            Err(_) => Err(Error::EventConflict {
                event: "<waterfall>".to_owned(),
                reason: "waterfall response type changed inside the chain".to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::ControlFlow;
    use std::sync::atomic::Ordering;

    struct Ping {
        seq: u32,
    }

    #[tokio::test]
    async fn waterfall_next_runs_middleware_then_final() {
        // m1 wraps m2 wraps the final; payload flows down, results up.
        let m2: Arc<ListenerHandler> = Arc::new(ListenerHandler::Waterfall(Box::new(
            |payload: EventPayload, next: NextErased| {
                Box::pin(async move {
                    let Ping { seq } = *payload.downcast::<Ping>().expect("payload");
                    let inner = next.run(Box::new(Ping { seq: seq + 1 })).await?;
                    let text = *inner.downcast::<String>().expect("response");
                    Ok(Box::new(format!("m2({text})")) as EventResponse)
                })
            },
        )));
        let m1: Arc<ListenerHandler> = Arc::new(ListenerHandler::Waterfall(Box::new(
            |payload: EventPayload, next: NextErased| {
                Box::pin(async move {
                    let inner = next.run(payload).await?;
                    let text = *inner.downcast::<String>().expect("response");
                    Ok(Box::new(format!("m1({text})")) as EventResponse)
                })
            },
        )));
        let final_: WaterfallFinal = Box::new(|payload: EventPayload| {
            Box::pin(async move {
                let Ping { seq } = *payload.downcast::<Ping>().expect("payload");
                Ok(Box::new(format!("final({seq})")) as EventResponse)
            })
        });

        let head = NextErased::new(vec![m1, m2].into(), final_);
        let result = head.run(Box::new(Ping { seq: 7 })).await.expect("runs");
        let text = *result.downcast::<String>().expect("response");
        assert_eq!(text, "m1(m2(final(8)))");
    }

    #[tokio::test]
    async fn waterfall_short_circuit_skips_the_final() {
        let used = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&used);
        let middleware: Arc<ListenerHandler> = Arc::new(ListenerHandler::Waterfall(Box::new(
            move |_payload: EventPayload, _next: NextErased| {
                let counter = Arc::clone(&counter);
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    // Never calls next: short-circuit.
                    Ok(Box::new("short".to_owned()) as EventResponse)
                })
            },
        )));
        let final_runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let final_counter = Arc::clone(&final_runs);
        let final_: WaterfallFinal = Box::new(move |_payload: EventPayload| {
            let final_counter = Arc::clone(&final_counter);
            Box::pin(async move {
                final_counter.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new("final".to_owned()) as EventResponse)
            })
        });

        let head = NextErased::new(vec![middleware].into(), final_);
        let result = head.run(Box::new(Ping { seq: 1 })).await.expect("runs");
        assert_eq!(*result.downcast::<String>().expect("response"), "short");
        assert_eq!(used.load(Ordering::SeqCst), 1);
        assert_eq!(final_runs.load(Ordering::SeqCst), 0, "final skipped");
    }

    #[tokio::test]
    async fn typed_next_downcasts_flow() {
        let final_: WaterfallFinal = Box::new(|_payload: EventPayload| {
            Box::pin(async { Ok(Box::new(42u32) as EventResponse) })
        });
        let next: Next<Ping, u32> = Next::new(NextErased::new(vec![].into(), final_));
        assert_eq!(next.run(Ping { seq: 0 }).await.expect("typed"), 42);
        let _ = ControlFlow::<u32>::Continue(());
    }
}
