//! Typed config model mirroring `config.toml`. Split into app-level (`app`) and
//! per-chain (`chain`) settings; both re-exported flat as `config::*`.

mod app;
mod chain;

pub use app::*;
pub use chain::*;
