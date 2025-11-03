use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use dashmap::DashSet;
use pingora_core::apps::ServerApp;
use pingora_core::connectors::TransportConnector;
use pingora_core::protocols::Stream;
use pingora_core::server::ShutdownWatch;
use pingora_core::upstreams::peer::{BasicPeer, Peer};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tracing::debug;

use crate::backend_pool::SimpleBackendPool;
use crate::upstream::ProxyMetadata;

/// Forward proxy that distributes requests across upstream proxies
pub struct ForwardProxy {
    pool: Arc<SimpleBackendPool>,
    banned: Arc<DashSet<String>>,
    connector: TransportConnector,
}

impl ForwardProxy {
    const LB_MAX_ITERATIONS: usize = 256;
    const HEADER_LIMIT: usize = 64 * 1024;
    const REQUEST_TIMEOUT_SECS: u64 = 30;
    const RESPONSE_TIMEOUT_SECS: u64 = 30;

    /// Creates a new ForwardProxy
    pub fn new(pool: Arc<SimpleBackendPool>) -> Self {
        Self {
            pool,
            banned: Arc::new(DashSet::new()),
            connector: TransportConnector::new(None),
        }
    }

    fn request_timeout() -> Duration {
        Duration::from_secs(Self::REQUEST_TIMEOUT_SECS)
    }

    fn response_timeout() -> Duration {
        Duration::from_secs(Self::RESPONSE_TIMEOUT_SECS)
    }

    /// Handles a complete proxy connection: reads request, selects backend, proxies with retries
    async fn handle_connection(
        &self,
        mut downstream: Stream,
        shutdown: &ShutdownWatch,
    ) -> Result<()> {
        if *shutdown.borrow() {
            debug!("Shutdown in progress; rejecting new downstream session");
            return Ok(());
        }

        let initial = match Self::read_initial_request(&mut downstream).await {
            Ok(req) => req,
            Err(err) => {
                debug!("Failed to read initial downstream request: {err:#}");
                return Err(err);
            }
        };

        let total_backends = self.pool.len();
        if total_backends == 0 {
            debug!("No upstream proxies configured; returning 503");
            Self::respond_service_unavailable(&mut downstream).await?;
            bail!("no upstream proxies configured");
        }

        let mut attempted = HashSet::new();
        let mut last_error: Option<anyhow::Error> = None;

        // Try proxying through available backends
        for _ in 0..Self::LB_MAX_ITERATIONS {
            if attempted.len() >= total_backends {
                break;
            }

            let backend = match self.pool.select() {
                Some(b) => b,
                None => break,
            };

            let backend_addr = backend.addr.to_string();

            if self.banned.contains(&backend_addr) {
                attempted.insert(backend_addr);
                continue;
            }

            if !attempted.insert(backend_addr.clone()) {
                continue;
            }

            let metadata = match backend.ext.get::<ProxyMetadata>() {
                Some(m) => m.clone(),
                None => {
                    debug!("Backend {backend_addr} missing metadata; skipping");
                    continue;
                }
            };

            match self
                .try_proxy_once(&backend_addr, &metadata, &initial)
                .await
            {
                Ok(AttemptOutcome::Success {
                    mut upstream,
                    response_header,
                    response_body_prefix,
                    status_code,
                }) => {
                    downstream
                        .write_all(&response_header)
                        .await
                        .context("failed to forward upstream response header")?;
                    if !response_body_prefix.is_empty() {
                        downstream
                            .write_all(&response_body_prefix)
                            .await
                            .context("failed to forward upstream response body prefix")?;
                    }
                    downstream
                        .flush()
                        .await
                        .context("failed to flush downstream response")?;

                    if initial.is_connect {
                        debug!("CONNECT tunnel established via {backend_addr}");
                    } else {
                        debug!("Forwarded response from {backend_addr} with status {status_code}");
                    }

                    if matches!(status_code, 407 | 402 | 511) {
                        debug!(
                            "Auth failure status surfaced in success branch; banning {backend_addr}"
                        );
                        self.banned.insert(backend_addr);
                        return Ok(());
                    }

                    if let Err(err) = io::copy_bidirectional(&mut downstream, &mut upstream).await {
                        debug!("Bidirectional stream with {backend_addr} terminated: {err}");
                    }

                    let _ = downstream.shutdown().await;
                    let _ = upstream.shutdown().await;

                    return Ok(());
                }
                Ok(AttemptOutcome::Retry {
                    status_code,
                    banned_count,
                }) => {
                    debug!(
                        "Proxy {backend_addr} returned {status_code}; banned (total banned: {banned_count}), retrying"
                    );
                    continue;
                }
                Err(err) => {
                    debug!("Attempt with {backend_addr} failed: {err:#}");
                    last_error = Some(err);
                    continue;
                }
            }
        }

        Self::respond_service_unavailable(&mut downstream).await?;
        if let Some(err) = last_error {
            Err(err)
        } else {
            bail!("no healthy non-banned proxies available")
        }
    }

    /// Reads and parses the initial HTTP request from client
    async fn read_initial_request(stream: &mut Stream) -> Result<InitialRequest> {
        let (header, body_prefix) =
            Self::read_http_message(stream, Self::HEADER_LIMIT, Self::request_timeout())
                .await
                .context("failed to read downstream request header")?;

        let is_connect = Self::is_connect_request(&header)?;
        Ok(InitialRequest {
            header,
            body_prefix,
            is_connect,
        })
    }

    /// Reads HTTP message (header + initial body) from stream with timeout
    async fn read_http_message(
        stream: &mut Stream,
        limit: usize,
        read_timeout: Duration,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut buffer = Vec::with_capacity(1024);
        let mut chunk = [0u8; 4096];

        loop {
            let n = timeout(read_timeout, stream.read(&mut chunk))
                .await
                .context("timeout while reading message")?? as usize;

            if n == 0 {
                bail!("connection closed while reading message");
            }

            buffer.extend_from_slice(&chunk[..n]);

            if buffer.len() > limit {
                bail!("message header exceeded {limit} bytes");
            }

            if let Some(pos) = Self::find_header_end(&buffer) {
                let header_end = pos + 4;
                let body = buffer.split_off(header_end);
                return Ok((buffer, body));
            }
        }
    }

    /// Finds the end of HTTP headers (\r\n\r\n)
    fn find_header_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|window| window == b"\r\n\r\n")
    }

    /// Returns true if request is HTTP CONNECT method
    fn is_connect_request(header: &[u8]) -> Result<bool> {
        let text = std::str::from_utf8(header).context("request header is not valid utf-8")?;
        let mut lines = text.split("\r\n");
        let request_line = lines.next().unwrap_or_default();
        let method = request_line.split_whitespace().next().unwrap_or_default();
        Ok(method.eq_ignore_ascii_case("CONNECT"))
    }

    /// Adds Proxy-Authorization header if auth is provided
    fn build_request_header(original: &[u8], auth: Option<&str>) -> Result<Vec<u8>> {
        let Some(auth_value) = auth else {
            return Ok(original.to_vec());
        };

        if original.len() < 4 || &original[original.len() - 4..] != b"\r\n\r\n" {
            bail!("malformed HTTP header");
        }

        let header_without_blank = &original[..original.len() - 4];
        let header_text = std::str::from_utf8(header_without_blank)
            .context("request header is not valid utf-8")?;

        if header_text.lines().any(|line| {
            line.trim_start()
                .to_ascii_lowercase()
                .starts_with("proxy-authorization:")
        }) {
            return Ok(original.to_vec());
        }

        const PROXY_AUTH_PREFIX: &str = "Proxy-Authorization: ";

        let mut new_header =
            Vec::with_capacity(original.len() + PROXY_AUTH_PREFIX.len() + auth_value.len() + 4);
        new_header.extend_from_slice(header_without_blank);
        new_header.extend_from_slice(b"\r\n");
        new_header.extend_from_slice(PROXY_AUTH_PREFIX.as_bytes());
        new_header.extend_from_slice(auth_value.as_bytes());
        new_header.extend_from_slice(b"\r\n\r\n");
        Ok(new_header)
    }

    /// Parses HTTP status code from response header
    fn parse_status_code(header: &[u8]) -> Result<u16> {
        let text = std::str::from_utf8(header).context("response header is not valid utf-8")?;
        let mut lines = text.split("\r\n");
        let status_line = lines
            .next()
            .ok_or_else(|| anyhow!("empty response header"))?;
        let mut parts = status_line.split_whitespace();
        let _ = parts
            .next()
            .ok_or_else(|| anyhow!("missing HTTP version"))?;
        let status = parts
            .next()
            .ok_or_else(|| anyhow!("missing status code"))?
            .parse()
            .context("invalid status code")?;
        Ok(status)
    }

    /// Sends 503 Service Unavailable to client
    async fn respond_service_unavailable(stream: &mut Stream) -> Result<()> {
        const BODY: &str = "Service Unavailable";
        let response = format!(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n{}",
            BODY.len(),
            BODY
        );
        stream
            .write_all(response.as_bytes())
            .await
            .context("failed to write fallback response")?;
        stream
            .flush()
            .await
            .context("failed to flush fallback response")?;
        Ok(())
    }

    /// Attempts to proxy request through a single backend
    async fn try_proxy_once(
        &self,
        backend_addr: &str,
        metadata: &ProxyMetadata,
        initial: &InitialRequest,
    ) -> Result<AttemptOutcome> {
        let mut peer = BasicPeer::new(backend_addr);
        if metadata.scheme.eq_ignore_ascii_case("https") {
            peer.sni = metadata.host.clone();
        }
        if let Some(opts) = peer.get_mut_peer_options() {
            opts.verify_cert = false;
            opts.verify_hostname = false;
            opts.set_http_version(1, 1);
        }

        let (mut upstream, _reused) = self
            .connector
            .get_stream(&peer)
            .await
            .with_context(|| format!("failed to connect to upstream {backend_addr}"))?;

        let auth_header = metadata.basic_auth_header();
        let request_header = Self::build_request_header(&initial.header, auth_header.as_deref())?;

        debug!("Sending request to {backend_addr}, header length: {}", request_header.len());
        debug!("Request header: {}", String::from_utf8_lossy(&request_header));

        upstream
            .write_all(&request_header)
            .await
            .with_context(|| format!("failed to send request header to {backend_addr}"))?;
        if !initial.body_prefix.is_empty() {
            upstream
                .write_all(&initial.body_prefix)
                .await
                .with_context(|| format!("failed to send request body prefix to {backend_addr}"))?;
        }
        upstream
            .flush()
            .await
            .with_context(|| format!("failed to flush request to {backend_addr}"))?;

        let (response_header, response_body_prefix) = match Self::read_http_message(
            &mut upstream,
            Self::HEADER_LIMIT,
            Self::response_timeout(),
        )
        .await
        {
            Ok(data) => data,
            Err(err) => {
                return Err(err.context(format!(
                    "failed to read response header from {backend_addr}"
                )));
            }
        };

        let status = Self::parse_status_code(&response_header)
            .with_context(|| format!("failed to parse response status from {backend_addr}"))?;

        // Ban backends that return auth failure codes
        if matches!(status, 407 | 402 | 511) {
            self.banned.insert(backend_addr.to_string());
            return Ok(AttemptOutcome::Retry {
                status_code: status,
                banned_count: self.banned.len(),
            });
        }

        Ok(AttemptOutcome::Success {
            upstream,
            response_header,
            response_body_prefix,
            status_code: status,
        })
    }
}

/// Initial HTTP request from client
struct InitialRequest {
    header: Vec<u8>,
    body_prefix: Vec<u8>,
    is_connect: bool,
}

/// Result of proxy attempt
enum AttemptOutcome {
    /// Success - connection established
    Success {
        upstream: Stream,
        response_header: Vec<u8>,
        response_body_prefix: Vec<u8>,
        status_code: u16,
    },
    /// Retry with different backend
    Retry {
        status_code: u16,
        banned_count: usize,
    },
}

#[async_trait]
impl ServerApp for ForwardProxy {
    /// Handles new client connection
    async fn process_new(self: &Arc<Self>, io: Stream, shutdown: &ShutdownWatch) -> Option<Stream> {
        if let Err(err) = self.handle_connection(io, shutdown).await {
            debug!("Connection failed: {err:#}");
        }
        None
    }
}
