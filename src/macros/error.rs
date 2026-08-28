/// Emit a structured ECS Core error with its exact source location.
#[macro_export]
macro_rules! report_error {
    ($reason:expr, $context_type:expr, $context_name:expr) => {{
        let reason = ::std::format!("{}", $reason);
        let context_type = ::std::format!("{}", $context_type);
        let context_name = ::std::format!("{}", $context_name);
        $crate::__private::tracing::error!(
            reason = %reason,
            context_type = %context_type,
            context_name = %context_name,
            module = module_path!(),
            file = file!(),
            line = line!(),
            "ECS operation failed",
        );
    }};
}

/// Emit a structured ECS Core degraded-operation warning.
#[macro_export]
macro_rules! report_warning {
    ($reason:expr, $context_type:expr, $context_name:expr) => {{
        let reason = ::std::format!("{}", $reason);
        let context_type = ::std::format!("{}", $context_type);
        let context_name = ::std::format!("{}", $context_name);
        $crate::__private::tracing::warn!(
            reason = %reason,
            context_type = %context_type,
            context_name = %context_name,
            module = module_path!(),
            file = file!(),
            line = line!(),
            "ECS operation degraded",
        );
    }};
}

/// Panic only when an internal ECS Core invariant is impossible to preserve.
///
/// User input, disk errors, network errors, capacity exhaustion, cancellation,
/// and insufficient shards must be returned as [`EcsError`](crate::EcsError).
#[macro_export]
macro_rules! fatal_error {
    ($reason:expr, $context_type:expr, $context_name:expr) => {{
        panic!(
            "fatal ECS invariant: '{}' | {}: '{}' | module: '{}' | file: '{}' | line: {}",
            $reason,
            $context_type,
            $context_name,
            module_path!(),
            file!(),
            line!(),
        );
    }};
}
