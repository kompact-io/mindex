//! scheme: which wire scheme a `Client` talks, and how to build one
//! from flags -- the one place `floe-client` and `floe-smoke` share
//! "pick a scheme, connect that client" instead of each reimplementing
//! it. `SchemeArgs`' shape mirrors `floe-server`'s own `--grpc`/
//! `--http`+`--http-json`/`--http-cbor` exactly
//! (`crates/server/src/main.rs`'s `RunArgs`).

use std::time::Duration;
#[cfg(feature = "grpc")]
use std::time::Instant;

use anyhow::Result;

pub use floe_dto_http::health::Scheme;

use crate::common::Backend;

/// `--grpc`/`--http`+`--http-json`/`--http-cbor`.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "clap", derive(clap::Args))]
pub struct SchemeArgs {
    /// gRPC address, host:port (or a full URL). Its own scheme,
    /// independent of `--http`.
    #[cfg_attr(feature = "clap", arg(long))]
    pub grpc: Option<String>,
    /// http address, host:port -- one listener, same as `floe-server`'s
    /// own `--http`. Requires `--http-json` and/or `--http-cbor` to say
    /// which scheme(s) to dial against it.
    #[cfg_attr(feature = "clap", arg(long))]
    pub http: Option<String>,
    /// Dial `--http` as the `http-json` scheme (`Accept:
    /// application/json`). Requires `--http`.
    #[cfg_attr(feature = "clap", arg(long, requires = "http"))]
    pub http_json: bool,
    /// Dial `--http` as the `http-cbor` scheme (`Accept:
    /// application/cbor`). Requires `--http`.
    #[cfg_attr(feature = "clap", arg(long, requires = "http"))]
    pub http_cbor: bool,
}

impl SchemeArgs {
    /// Every scheme these flags ask for.
    pub fn selected(&self) -> Vec<Scheme> {
        let mut schemes = Vec::new();
        if self.grpc.is_some() {
            schemes.push(Scheme::Grpc);
        }
        if self.http_json {
            schemes.push(Scheme::HttpJson);
        }
        if self.http_cbor {
            schemes.push(Scheme::HttpCbor);
        }
        schemes
    }

    /// Exactly one scheme, or an error -- for a one-shot query tool
    /// that can only act against a single connection: the client-side
    /// mirror of the server's own runtime `negotiate()`, which also
    /// picks exactly one wire format per request.
    pub fn selected_one(&self) -> Result<Scheme> {
        match self.selected().as_slice() {
            [scheme] => Ok(*scheme),
            [] => anyhow::bail!(
                "select exactly one scheme: --grpc, or --http with --http-json/--http-cbor"
            ),
            many => anyhow::bail!(
                "select exactly one scheme, not several: {}",
                many.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

/// Prepends `http://` to a bare `host:port`; passes an already-schemed
/// value through.
#[cfg(feature = "grpc")]
fn grpc_url(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// gRPC connect fails outright if the port isn't listening yet, unlike
/// HTTP's `Client::new` -- needs its own retry loop.
#[cfg(feature = "grpc")]
async fn connect_grpc_with_retry(
    server: &str,
    ready_timeout: Duration,
) -> Result<crate::grpc::Client> {
    let start = Instant::now();
    loop {
        match crate::grpc::Client::connect(server).await {
            Ok(client) => return Ok(client),
            Err(_) if start.elapsed() < ready_timeout => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => anyhow::bail!("never connected within {ready_timeout:?}: {e}"),
        }
    }
}

/// Type-erases across the (up to three) concrete `Client`s this crate
/// compiles in -- `Backend` itself isn't object-safe (its methods are
/// `async fn`s), so this enum plus a hand-delegated `impl Backend`
/// stands in for `Box<dyn Backend>`.
pub enum AnyClient {
    // Boxed: `grpc::Client` (two tonic channels) dwarfs the other two
    // (a bare address `String` each) -- unboxed, every `AnyClient`
    // would pay the gRPC variant's size regardless of which scheme it
    // holds.
    #[cfg(feature = "grpc")]
    Grpc(Box<crate::grpc::Client>),
    #[cfg(feature = "http-json")]
    HttpJson(crate::http_json::Client),
    #[cfg(feature = "http-cbor")]
    HttpCbor(crate::http_cbor::Client),
}

macro_rules! delegate {
    ($self:expr, |$c:ident| $call:expr) => {
        match $self {
            #[cfg(feature = "grpc")]
            AnyClient::Grpc($c) => $call,
            #[cfg(feature = "http-json")]
            AnyClient::HttpJson($c) => $call,
            #[cfg(feature = "http-cbor")]
            AnyClient::HttpCbor($c) => $call,
            // Reachable only if `AnyClient` is uninhabited (no scheme
            // feature compiled in) -- a reference to an empty enum
            // isn't exhaustiveness-checked as uninhabited by itself.
            #[allow(unreachable_patterns)]
            _ => unreachable!("no scheme feature compiled in"),
        }
    };
}

// Unused only with zero scheme features compiled in (e.g. `wasm` alone) --
// every real build has at least one and uses these for real.
#[allow(unused_variables)]
impl Backend for AnyClient {
    async fn read_utxos(
        &mut self,
        req: floe_dto_http::query::ReadUtxosRequest,
    ) -> Result<floe_dto_http::query::ReadUtxosResponse> {
        delegate!(self, |c| c.read_utxos(req).await)
    }
    async fn search_utxos(
        &mut self,
        req: floe_dto_http::query::SearchUtxosRequest,
    ) -> Result<floe_dto_http::query::SearchUtxosResponse> {
        delegate!(self, |c| c.search_utxos(req).await)
    }
    async fn read_data(
        &mut self,
        req: floe_dto_http::query::ReadDataRequest,
    ) -> Result<Option<floe_dto_http::query::ReadDataResponse>> {
        delegate!(self, |c| c.read_data(req).await)
    }
    async fn read_tx(
        &mut self,
        req: floe_dto_http::query::ReadTxRequest,
    ) -> Result<Option<floe_dto_http::query::ReadTxResponse>> {
        delegate!(self, |c| c.read_tx(req).await)
    }
    async fn submit_tx(&mut self, raw: Vec<u8>) -> Result<floe_dto_http::submit::SubmitTxResponse> {
        delegate!(self, |c| c.submit_tx(raw).await)
    }
    async fn get_health(&mut self) -> Result<floe_dto_http::health::HealthResponse> {
        delegate!(self, |c| c.get_health().await)
    }
    async fn get_metrics(&mut self) -> Result<floe_dto_http::health::MetricsResponse> {
        delegate!(self, |c| c.get_metrics().await)
    }
}

/// Connects the concrete `Client` for `scheme` -- gRPC retries until
/// listening (`ready_timeout`); http just builds the address-only
/// client. Each scheme gated by its own compile-time feature, matching
/// `floe-server`'s own "not compiled in" `bail!` message style.
#[allow(unused_variables)] // `args`/`ready_timeout` unused when no scheme feature is compiled in
pub async fn connect(
    scheme: Scheme,
    args: &SchemeArgs,
    ready_timeout: Duration,
) -> Result<AnyClient> {
    match scheme {
        Scheme::Grpc => {
            #[cfg(feature = "grpc")]
            {
                let server = grpc_url(
                    args.grpc
                        .as_deref()
                        .expect("Scheme::Grpc only chosen when --grpc is given"),
                );
                Ok(AnyClient::Grpc(Box::new(
                    connect_grpc_with_retry(&server, ready_timeout).await?,
                )))
            }
            #[cfg(not(feature = "grpc"))]
            anyhow::bail!("--grpc: not compiled in (missing the `grpc` feature)");
        }
        Scheme::HttpJson => {
            #[cfg(feature = "http-json")]
            {
                let server = args.http.as_deref().expect(
                    "Scheme::HttpJson only chosen when --http-json (requires --http) is given",
                );
                Ok(AnyClient::HttpJson(crate::http_json::Client::new(server)))
            }
            #[cfg(not(feature = "http-json"))]
            anyhow::bail!("--http-json: not compiled in (missing the `http-json` feature)");
        }
        Scheme::HttpCbor => {
            #[cfg(feature = "http-cbor")]
            {
                let server = args.http.as_deref().expect(
                    "Scheme::HttpCbor only chosen when --http-cbor (requires --http) is given",
                );
                Ok(AnyClient::HttpCbor(crate::http_cbor::Client::new(server)))
            }
            #[cfg(not(feature = "http-cbor"))]
            anyhow::bail!("--http-cbor: not compiled in (missing the `http-cbor` feature)");
        }
    }
}
