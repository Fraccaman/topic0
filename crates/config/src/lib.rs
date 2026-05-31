//! Load + validate `config.toml` (with `${ENV}` interpolation) into `Config`.

mod model;
mod validate;

pub use model::*;

use figment::{
    providers::{Env, Format, Toml},
    Figment,
};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config: {0}")]
    Read(#[from] figment::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

/// Load from a TOML path, overlaying `INDEXER_`-prefixed env vars, then validate.
#[allow(clippy::result_large_err)]
pub fn load(path: &str) -> Result<Config, ConfigError> {
    let cfg: Config = Figment::new()
        .merge(Toml::file(path))
        .merge(Env::prefixed("INDEXER_").split("__"))
        .extract()?;
    validate::validate(&cfg).map_err(ConfigError::Invalid)?;
    Ok(cfg)
}

/// Parse + validate from a TOML string.
#[allow(clippy::result_large_err)]
pub fn from_str(s: &str) -> Result<Config, ConfigError> {
    let cfg: Config = Figment::new().merge(Toml::string(s)).extract()?;
    validate::validate(&cfg).map_err(ConfigError::Invalid)?;
    Ok(cfg)
}
