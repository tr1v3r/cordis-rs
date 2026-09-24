//! Public error types of the cordis-rs kernel.
//!
//! The framework error ([`Error`]) uses an explicit enum: callers must be
//! able to distinguish lifecycle refusals (closed hosts, stale generations)
//! from configuration and service problems without parsing strings. Error
//! values carry ids and reasons only — never configuration payloads — so
//! they are safe to log and to implement [`Send`] + [`Sync`].

use std::error::Error as StdError;
use std::fmt;

use crate::id::FiberId;

/// Framework-level error returned by cordis-core APIs.
///
/// Variants follow the taxonomy of the design documents (docs/02-api.md §8).
/// The enum is `#[non_exhaustive]`: later phases (services, events, loader
/// integration) add variants without it being a breaking change.
#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    /// The host [`App`](crate::App) was shut down or dropped; no further
    /// admissions or reads are accepted.
    HostClosed,
    /// The target effect scope is sealed and no longer accepts registrations.
    InactiveScope,
    /// The fiber or generation a handle points at no longer exists in its
    /// app. Full generation semantics arrive with the P2 coordinator.
    StaleGeneration {
        /// The fiber whose record could not be found.
        fiber: FiberId,
    },
    /// A configuration value or builder input is invalid. `reason` is a
    /// kernel-generated message and never contains configuration payloads.
    InvalidConfig {
        /// Why the configuration was rejected.
        reason: String,
    },
    /// A service was consumed without being declared as a dependency.
    UndeclaredDependency {
        /// Name of the service.
        service: String,
    },
    /// A declared service is not currently provided in the resolved
    /// namespace.
    ///
    /// `namespace` is the rendered [`ScopeId`](crate::ScopeId) the lookup
    /// resolved to (for example `default` or `unique(7)`). Isolated views
    /// report the isolated namespace here instead of falling back to a
    /// parent namespace.
    ServiceMissing {
        /// Name of the service.
        service: String,
        /// The namespace that was searched, as rendered by the scope.
        namespace: String,
    },
    /// A service is registered or requested under a different type than the
    /// one declared for its key.
    ServiceTypeMismatch {
        /// Name of the service.
        service: String,
        /// The namespace the binding was found in.
        namespace: String,
    },
    /// Another provider already occupies the service slot in this scope.
    ServiceExists {
        /// Name of the service.
        service: String,
        /// The namespace of the occupied slot.
        namespace: String,
    },
    /// A dependency declaration is inconsistent (for example the same name
    /// declared twice with different expected types).
    InvalidDependency {
        /// Name of the service whose declaration conflicts.
        service: String,
        /// Why the declaration was rejected.
        reason: String,
    },
    /// The binding a lease was pinned to has been retired; new snapshots
    /// are refused. Arcs already taken out of the lease remain valid.
    ServiceRetired {
        /// The retired binding.
        binding: crate::id::BindingId,
    },
    /// A registration is being operated on by a scope that does not own it.
    InvalidOwner,
    /// Waiting for an operation from inside that operation's own lifecycle
    /// callback would deadlock; the wait was refused.
    WouldDeadlock,
    /// A plugin activation (apply future) failed.
    ActivationFailed {
        /// The error returned by user plugin code.
        source: PluginError,
    },
    /// A supervised task finished with an error.
    TaskFailed {
        /// The error produced by the task.
        source: Box<dyn StdError + Send + Sync>,
    },
    /// A cleanup step failed; other independent resources are still cleaned
    /// and the aggregate is reported through
    /// [`CleanupReport`](crate::CleanupReport).
    CleanupFailed {
        /// The error returned by the cleanup step.
        source: CleanupError,
    },
    /// Resources could not be confirmed released (for example an
    /// uncooperative task); they are quarantined instead of being reported
    /// as disposed.
    Quarantined {
        /// What could not be released, and why.
        reason: String,
    },
    /// An admission limit (mailbox backlog, live fibers, live workers) was
    /// reached; the request was refused instead of being silently queued
    /// without bound (docs/03-runtime.md I13).
    CapacityExceeded {
        /// Which limit was hit and its size.
        reason: String,
    },
    /// A supervised worker crossed its panic boundary: the factory that
    /// constructs the activation future, or the future itself, panicked.
    /// The panic is recorded as a failure of the owning fiber; the
    /// coordinator itself keeps running.
    WorkerPanicked {
        /// Where the panic crossed the boundary (factory or poll).
        context: String,
        /// The panic payload rendered as a message when possible.
        message: String,
    },
    /// A deadline passed before the awaited state was reached. The deadline
    /// says nothing about whether the underlying work eventually finished
    /// (docs/03-runtime.md §8).
    DeadlineExceeded {
        /// What was being waited for.
        reason: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::HostClosed => write!(f, "the host app is shut down or dropped"),
            Error::InactiveScope => write!(f, "the effect scope no longer accepts registrations"),
            Error::StaleGeneration { fiber } => write!(
                f,
                "the target of this handle no longer exists in its app (fiber {fiber:?})"
            ),
            Error::InvalidConfig { reason } => write!(f, "invalid configuration: {reason}"),
            Error::UndeclaredDependency { service } => write!(
                f,
                "service {service:?} is used but was not declared as a dependency"
            ),
            Error::ServiceMissing { service, namespace } => write!(
                f,
                "service {service:?} is not currently provided in namespace {namespace}"
            ),
            Error::ServiceTypeMismatch { service, namespace } => write!(
                f,
                "service {service:?} is bound with a different type than requested in namespace {namespace}"
            ),
            Error::ServiceExists { service, namespace } => write!(
                f,
                "service {service:?} already has a provider in namespace {namespace}"
            ),
            Error::InvalidDependency { service, reason } => {
                write!(
                    f,
                    "invalid dependency declaration for {service:?}: {reason}"
                )
            }
            Error::ServiceRetired { binding } => write!(
                f,
                "service binding {binding:?} is retired; leases no longer serve snapshots"
            ),
            Error::InvalidOwner => write!(f, "the registration is not owned by this scope"),
            Error::WouldDeadlock => write!(
                f,
                "waiting for this operation from inside its own callback would deadlock"
            ),
            Error::ActivationFailed { .. } => write!(f, "plugin activation failed"),
            Error::TaskFailed { .. } => write!(f, "a supervised task failed"),
            Error::CleanupFailed { .. } => write!(f, "a cleanup step failed"),
            Error::Quarantined { reason } => {
                write!(
                    f,
                    "resources could not be released and are quarantined: {reason}"
                )
            }
            Error::CapacityExceeded { reason } => {
                write!(f, "an admission limit was reached: {reason}")
            }
            Error::WorkerPanicked { context, message } => {
                write!(f, "a supervised worker panicked in {context}: {message}")
            }
            Error::DeadlineExceeded { reason } => {
                write!(f, "a deadline passed before {reason}")
            }
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::ActivationFailed { source } => Some(source),
            Error::TaskFailed { source } => Some(source.as_ref()),
            Error::CleanupFailed { source } => Some(source),
            _ => None,
        }
    }
}

/// Error type returned by user plugin code (apply futures, validators).
///
/// A thin wrapper over a boxed error, so plugins can surface their own error
/// types without the kernel tying itself to any specific error crate.
#[derive(Debug)]
pub struct PluginError(Box<dyn StdError + Send + Sync>);

impl PluginError {
    /// Wraps an error value implementing
    /// `Into<Box<dyn std::error::Error + Send + Sync>>`.
    pub fn new<E>(error: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync>>,
    {
        Self(error.into())
    }
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StdError for PluginError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

impl From<Error> for PluginError {
    /// Wraps a framework refusal so plugin bodies can propagate it with
    /// `?`: a service slot conflict, a stale scope or a missing
    /// dependency that the plugin cannot handle is an activation
    /// failure, and the original [`Error`] stays observable through
    /// `Display`/`source`.
    fn from(error: Error) -> Self {
        Self(Box::new(error))
    }
}

impl From<String> for PluginError {
    fn from(message: String) -> Self {
        Self(message.into())
    }
}

impl From<&str> for PluginError {
    fn from(message: &str) -> Self {
        Self(message.into())
    }
}

/// Error type returned by cleanup steps (effect disposers, managed service
/// stoppers). Cleanup errors are aggregated into a
/// [`CleanupReport`](crate::CleanupReport) instead of short-circuiting
/// independent resources.
#[derive(Debug)]
pub struct CleanupError(Box<dyn StdError + Send + Sync>);

impl CleanupError {
    /// Wraps an error value implementing
    /// `Into<Box<dyn std::error::Error + Send + Sync>>`.
    pub fn new<E>(error: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync>>,
    {
        Self(error.into())
    }
}

impl fmt::Display for CleanupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StdError for CleanupError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

impl From<String> for CleanupError {
    fn from(message: String) -> Self {
        Self(message.into())
    }
}

impl From<&str> for CleanupError {
    fn from(message: &str) -> Self {
        Self(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_uses_stable_english_messages() {
        assert_eq!(
            Error::HostClosed.to_string(),
            "the host app is shut down or dropped"
        );
        let err = Error::InvalidConfig {
            reason: "app name must not be empty".to_owned(),
        };
        assert_eq!(
            err.to_string(),
            "invalid configuration: app name must not be empty"
        );
    }

    #[test]
    fn service_errors_report_the_namespace() {
        let missing = Error::ServiceMissing {
            service: "db".to_owned(),
            namespace: "unique(7)".to_owned(),
        };
        let text = missing.to_string();
        assert!(text.contains("\"db\""), "names the service: {text}");
        assert!(text.contains("unique(7)"), "names the namespace: {text}");

        let exists = Error::ServiceExists {
            service: "db".to_owned(),
            namespace: "default".to_owned(),
        };
        assert!(exists.to_string().contains("namespace default"));

        let retired = Error::ServiceRetired {
            binding: crate::id::BindingId::new(
                crate::id::FiberId::alloc_global(),
                crate::id::GenerationId::alloc_global(),
                3,
            ),
        };
        // Identity only: no configuration content in the rendered error.
        assert!(retired.to_string().contains("BindingId(fiber="));
    }

    #[test]
    fn wrapped_errors_chain_their_source() {
        let plugin_error = PluginError::from("boom");
        let err = Error::ActivationFailed {
            source: plugin_error,
        };
        assert_eq!(err.to_string(), "plugin activation failed");
        let source = StdError::source(&err).expect("source is present");
        assert_eq!(source.to_string(), "boom");
    }

    #[test]
    fn error_types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Error>();
        assert_send_sync::<PluginError>();
        assert_send_sync::<CleanupError>();
    }
}
