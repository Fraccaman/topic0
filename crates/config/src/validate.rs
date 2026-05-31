//! Structural validation beyond serde parsing.

use crate::model::Config;
use std::collections::HashSet;

pub fn validate(cfg: &Config) -> Result<(), String> {
    if cfg.database.url.is_empty() {
        return Err("database.url is required".into());
    }
    if cfg.queue.kind != "postgres" {
        return Err(format!(
            "queue.kind '{}' unsupported (only 'postgres')",
            cfg.queue.kind
        ));
    }
    if !matches!(cfg.query.expose.as_str(), "finalized" | "provisional") {
        return Err(format!("query.expose '{}' invalid", cfg.query.expose));
    }

    let mut seen = HashSet::new();
    for chain in &cfg.chains {
        if !seen.insert(chain.id) {
            return Err(format!("duplicate chain id {}", chain.id));
        }
        if chain.source.http.is_empty() {
            return Err(format!("chain {} source.http is required", chain.id));
        }
        if chain.start_block().is_none() {
            return Err(format!(
                "chain {} has no scan start: set start_block on at least one contract",
                chain.id
            ));
        }
        for c in &chain.contracts {
            if !c.address.starts_with("0x") || c.address.len() != 42 {
                return Err(format!(
                    "chain {} contract address '{}' malformed",
                    chain.id, c.address
                ));
            }
            if c.abi.is_empty() {
                return Err(format!(
                    "chain {} contract {} missing abi path",
                    chain.id, c.address
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    const SAMPLE: &str = r#"
[database]
url = "postgres://localhost/idx"

[[chains]]
id = 1
[chains.source]
kind = "alchemy"
http = "https://x"

[[chains.contracts]]
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
events = ["Transfer"]
start_block = 19000000
"#;

    #[test]
    fn parses_and_validates_sample() {
        let cfg = crate::from_str(SAMPLE).expect("valid config");
        assert_eq!(cfg.chains.len(), 1);
        assert_eq!(cfg.chains[0].source.kind, "alchemy");
        assert_eq!(cfg.chains[0].confirmations, 12);
        assert_eq!(cfg.chains[0].contracts[0].events, vec!["Transfer"]);
        assert_eq!(cfg.chains[0].start_block(), Some(19000000));
    }

    #[test]
    fn rejects_missing_start_block() {
        let bad = SAMPLE.replace("start_block = 19000000\n", "");
        assert!(crate::from_str(&bad).is_err());
    }

    #[test]
    fn rejects_bad_address() {
        let bad = SAMPLE.replace("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "0xdead");
        assert!(crate::from_str(&bad).is_err());
    }
}
