use tracing_subscriber::EnvFilter;

/// Installs the process-wide subscriber.
///
/// The only global in the binary; everything else is passed as a parameter.
pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
