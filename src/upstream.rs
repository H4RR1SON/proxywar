use std::{fs, net::ToSocketAddrs, path::Path};

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use pingora_load_balancing::Backend;
use url::Url;

/// Proxy backend metadata extracted from URL
#[derive(Debug, Clone)]
pub struct ProxyMetadata {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub original: String,
}

impl ProxyMetadata {
    /// Returns Basic auth header if username and password are present
    pub fn basic_auth_header(&self) -> Option<String> {
        match (&self.username, &self.password) {
            (Some(user), Some(pass)) => {
                let encoded = BASE64_ENGINE.encode(format!("{}:{}", user, pass));
                Some(format!("Basic {}", encoded))
            }
            _ => None,
        }
    }
}

/// Loads proxy backends from config file (one URL per line)
pub fn load_backends_from_file(path: impl AsRef<Path>) -> Result<Vec<Backend>> {
    let raw = fs::read_to_string(path.as_ref())
        .with_context(|| format!("failed to read proxy list from {}", path.as_ref().display()))?;

    let mut result = Vec::new();

    for (idx, line) in raw.lines().enumerate() {
        let trimmed = line.trim();

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let url = Url::parse(trimmed)
            .with_context(|| format!("invalid proxy url on line {}: {}", idx + 1, trimmed))?;

        let host = url
            .host_str()
            .with_context(|| format!("missing host for proxy url on line {}", idx + 1))?
            .to_string();

        let port = url
            .port_or_known_default()
            .ok_or_else(|| anyhow::anyhow!("missing port for proxy url on line {}", idx + 1))?;

        let socket_addr_str = format!("{}:{}", host, port);

        // Create backend, resolving DNS if needed
        let mut backend = match Backend::new(&socket_addr_str) {
            Ok(b) => b,
            Err(_) => {
                match socket_addr_str.to_socket_addrs() {
                    Ok(mut addrs) => {
                        if let Some(addr) = addrs.next() {
                            Backend::new(&addr.to_string()).with_context(|| {
                                format!(
                                    "failed to create backend for {} (resolved to {})",
                                    socket_addr_str, addr
                                )
                            })?
                        } else {
                            anyhow::bail!(
                                "could not resolve hostname {} on line {}",
                                host,
                                idx + 1
                            );
                        }
                    }
                    Err(e) => {
                        anyhow::bail!(
                            "could not resolve hostname {} on line {} - {}",
                            host,
                            idx + 1,
                            e
                        );
                    }
                }
            }
        };

        let username = if url.username().is_empty() {
            None
        } else {
            Some(url.username().to_string())
        };

        let metadata = ProxyMetadata {
            scheme: url.scheme().to_string(),
            host: host.clone(),
            port,
            username,
            password: url.password().map(|s| s.to_string()),
            original: trimmed.to_string(),
        };

        backend.ext.insert(metadata);
        result.push(backend);
    }

    if result.is_empty() {
        anyhow::bail!("no proxies loaded from {}", path.as_ref().display());
    }

    Ok(result)
}
