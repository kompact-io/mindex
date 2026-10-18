//! floe-smoke: deployment smoke test against a running `floe-server`
//! (not a correctness suite -- that's `floe-integration`). Exercises
//! the whole `Backend` contract (search by pay/stake hash, a
//! `since`-bounded search, `ReadUtxos`, `ReadTx`, `ReadData`,
//! `GetHealth`/`GetMetrics`, opt-in `SubmitTx` via `--submit`) and
//! fails on wiring problems, not absent data.
//!
//! Mirrors `floe-server`'s own `--grpc`/`--http`+`--http-json`/
//! `--http-cbor` shape exactly (`floe_client::SchemeArgs`, shared with
//! `floe-client`'s own CLI), rather than one address per wire format:
//! `--grpc <addr>` is its own scheme; `--http <addr>` is one address,
//! and `--http-json`/`--http-cbor` are bools picking which scheme(s) to
//! dial against *that* address. Whichever schemes that leaves run;
//! with two or more, their (ledger-tip-normalized) responses are
//! cross-checked for agreement.
//!
//! `--submit` chains every given scheme into one relay instead of each
//! submitting independently: scheme 0 submits a self-payment (see
//! `self_pay`), scheme 1 polls (via `SearchUtxos`'s `since`) until that
//! tx is visible to *it* and submits its own spending it, and so on --
//! closing the loop by having scheme 0 poll for the last scheme's tx
//! too (a horseshoe bent shut, see `submit_relay`). With one scheme
//! given, it closes on itself.
//!
//! Logging is `tracing`, `RUST_LOG`-filtered (default `info`); the
//! ready/connect retry loop stays at `debug`.
//!
//! usage:
//! ```text
//! cargo run -p floe-client --bin floe-smoke -- --grpc 127.0.0.1:50051 --skey "$FLOE_SMOKE_SKEY"
//! cargo run -p floe-client --bin floe-smoke -- --http 127.0.0.1:8080 --http-json --pay-hash <hash_hex>
//! cargo run -p floe-client --bin floe-smoke -- --http 127.0.0.1:8080 --http-cbor --address <bech32> --datum-hash <hash_hex>
//! cargo run -p floe-client --bin floe-smoke -- --grpc 127.0.0.1:50051 \
//!     --http 127.0.0.1:8080 --http-json --http-cbor --skey "$FLOE_SMOKE_SKEY" --submit
//! ```

mod self_pay;

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use pallas_addresses::Address;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use floe_client::{AnyClient, Backend, Hash28, NetworkArg, Scheme, SchemeArgs, TxHash, parse_hex};
use floe_dto_http::health::MetricsResponse;
use floe_dto_http::query::{
    ChainPoint, ReadDataRequest, ReadDataResponse, ReadTxRequest, ReadTxResponse, ReadUtxosRequest,
    SearchUtxosRequest, SearchUtxosResponse, TxoRef, Utxo,
};

/// Total requests recorded under `scheme`, across every method --
/// `run_smoke`'s own metrics check only needs "did our traffic get
/// counted", not a per-method breakdown.
fn total_requests(m: &MetricsResponse, scheme: Scheme) -> u64 {
    m.requests
        .iter()
        .filter(|r| r.scheme == scheme)
        .map(|r| r.count)
        .sum()
}

#[derive(Parser)]
#[command(
    name = "floe-smoke",
    about = "Smoke-test a running floe-server deployment"
)]
struct Cli {
    #[command(flatten)]
    scheme: SchemeArgs,

    /// Retry window before giving up on the server answering at all.
    #[arg(long, default_value_t = 30)]
    ready_timeout_secs: u64,

    /// ed25519 secret key, hex (32 bytes). Derives the search hash
    /// when `--pay-hash`/`--address` are absent; always signs
    /// `--submit`'s self-payment.
    #[arg(long, env = "FLOE_SMOKE_SKEY", value_parser = parse_hex::<32>)]
    skey: Option<[u8; 32]>,

    /// Bech32 network prefix for an address derived from `--skey`.
    #[arg(long, default_value = "mainnet")]
    network: NetworkArg,

    /// Address (bech32) to search. Overrides `--skey`'s derived one.
    #[arg(long)]
    address: Option<String>,

    /// Payment part hash to search, hex (28 bytes). Overrides
    /// `--address`/`--skey`.
    #[arg(long, value_parser = parse_hex::<28>)]
    pay_hash: Option<Hash28>,

    /// Stake part hash alongside `--pay-hash` (ignored otherwise).
    #[arg(long, value_parser = parse_hex::<28>)]
    stake_hash: Option<Hash28>,

    /// `ReadTx` this hash instead of the found UTxO's own tx.
    #[arg(long, value_parser = parse_hex::<32>)]
    tx_hash: Option<TxHash>,

    /// Also check `ReadData` on this hash.
    #[arg(long, value_parser = parse_hex::<32>)]
    datum_hash: Option<TxHash>,

    /// Also broadcast a self-payment per scheme given, chained in a
    /// relay (see this file's own module doc). Mutates chain state and
    /// spends real test ADA. Requires `--skey`.
    #[arg(long)]
    submit: bool,
}

/// `Fail` sinks the run's exit code; `Warn` doesn't (absent data, not
/// a wiring problem).
enum Step {
    Ok(String),
    Warn(String),
    Fail(String),
}

/// One-line UTxO summary, for cross-referencing against an independent
/// source.
fn precis_item(u: &Utxo) -> String {
    format!(
        "{}:{} coin={}",
        hex::encode(u.txo_ref.hash),
        u.txo_ref.index,
        u.output.coin
    )
}

/// `precis_item`'s `TxOutput`-only equivalent, for `ReadTx`'s
/// inputs/outputs (no `txo_ref`/`coin` wrapper the way a `Utxo` row
/// has -- see `floe_dto_http::query::Tx`'s own doc).
fn precis_output(o: &floe_dto_http::cardano::TxOutput) -> String {
    format!("coin={}", o.coin)
}

fn report(step: &Step) {
    match step {
        Step::Ok(msg) => info!("[ok]   {msg}"),
        Step::Warn(msg) => warn!("[warn] {msg}"),
        Step::Fail(msg) => error!("[fail] {msg}"),
    }
}

/// `--pay-hash` wins outright; otherwise `--address`/`--skey`'s
/// address is decomposed via `shelley_hashes`.
fn resolve_hash(cli: &Cli) -> Result<(Hash28, Option<Hash28>)> {
    if let Some(pay_hash) = cli.pay_hash {
        return Ok((pay_hash, cli.stake_hash));
    }
    let address: Address = if let Some(addr) = &cli.address {
        addr.parse().context("--address is not a valid address")?
    } else if let Some(skey) = cli.skey {
        let info = floe_client::key_info::key_info(skey, cli.network.0)?;
        info!(address = %info.address, "derived address from --skey");
        info.address
            .parse()
            .expect("key_info always returns a valid address")
    } else {
        anyhow::bail!("need one of --pay-hash, --address, or --skey to know what to search for");
    };
    floe_client::shelley_hashes(&address)
}

/// One scheme's results, for cross-scheme comparison in `main`.
#[derive(Default)]
struct SmokeOutcome {
    ok: bool,
    page: Option<SearchUtxosResponse>,
    reread: Option<Utxo>,
    stake_page: Option<SearchUtxosResponse>,
    tx: Option<ReadTxResponse>,
    datum: Option<ReadDataResponse>,
    since_page: Option<SearchUtxosResponse>,
}

/// Scheme-agnostic smoke flow. `SubmitTx` is exercised separately, by
/// `submit_relay` in `main` (chained across every scheme given, not a
/// per-scheme step here).
async fn run_smoke<B: Backend>(client: &mut B, cli: &Cli, scheme: Scheme) -> Result<SmokeOutcome> {
    let (pay_hash, stake_hash) = resolve_hash(cli)?;
    info!(
        scheme = %scheme,
        pay_hash = %hex::encode(pay_hash),
        stake_hash = ?stake_hash.map(hex::encode),
        "searching"
    );

    // "Ready" = first successful response (no dedicated health check).
    let timeout = Duration::from_secs(cli.ready_timeout_secs);
    let start = Instant::now();
    loop {
        match client
            .search_utxos(SearchUtxosRequest {
                pay_hash: Some(pay_hash),
                stake_hash: None,
                since_slot: None,
                since_hash: None,
                start_token: None,
                max_items: Some(1),
            })
            .await
        {
            Ok(_) => {
                info!(scheme = %scheme, elapsed = ?start.elapsed(), "server is answering");
                break;
            }
            Err(e) if start.elapsed() < timeout => {
                debug!(scheme = %scheme, error = %e, "not ready yet, retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => anyhow::bail!("[{scheme}] server never answered within {timeout:?}: {e}"),
        }
    }

    let mut outcome = SmokeOutcome {
        ok: true,
        ..Default::default()
    };
    let mut record = |step: Step| {
        if matches!(step, Step::Fail(_)) {
            outcome.ok = false;
        }
        report(&step);
    };

    // `GetHealth`/`GetMetrics`: not every scheme necessarily exposes
    // these (an `http-json`/`http-cbor` client only sees them if the
    // query listener and the dedicated `/health` listener happen to
    // share an address -- see `Backend::get_health`'s doc). Try, and if
    // unsupported here, warn and skip the metrics-reflects-this-run
    // check below rather than failing the whole scheme.
    let metrics_before: Option<MetricsResponse> = match client.get_health().await {
        Ok(h) => {
            record(Step::Ok(format!("[{scheme}] GetHealth: {h}")));
            match client.get_metrics().await {
                Ok(m) => {
                    record(Step::Ok(format!(
                        "[{scheme}] GetMetrics: utxo_count={} applied_total={}",
                        m.utxo_count, m.applied_total
                    )));
                    Some(m)
                }
                Err(e) => {
                    record(Step::Warn(format!(
                        "[{scheme}] GetMetrics unavailable: {e}"
                    )));
                    None
                }
            }
        }
        Err(e) => {
            record(Step::Warn(format!(
                "[{scheme}] GetHealth unavailable over this scheme ({e}) -- skipping"
            )));
            None
        }
    };

    let page = match client
        .search_utxos(SearchUtxosRequest {
            pay_hash: Some(pay_hash),
            stake_hash,
            since_slot: None,
            since_hash: None,
            start_token: None,
            max_items: None,
        })
        .await
    {
        Ok(page) => {
            let first = page.items.first().map(precis_item);
            record(Step::Ok(format!(
                "[{scheme}] SearchUtxos(pay_hash={}, stake_hash={:?}): {} item(s), first={:?}",
                hex::encode(pay_hash),
                stake_hash.map(hex::encode),
                page.items.len(),
                first
            )));
            Some(page)
        }
        Err(e) => {
            record(Step::Fail(format!("[{scheme}] SearchUtxos failed: {e}")));
            None
        }
    };
    let first = page.as_ref().and_then(|p| p.items.first()).cloned();
    outcome.page = page;

    let mut derived_tx_hash = None;
    if let Some(item) = &first {
        derived_tx_hash = Some(item.txo_ref.hash);
        match client
            .read_utxos(ReadUtxosRequest {
                keys: vec![TxoRef {
                    hash: item.txo_ref.hash,
                    index: item.txo_ref.index,
                }],
            })
            .await
            .map(|resp| resp.items.into_iter().next())
        {
            Ok(Some(reread)) if reread.output.coin == item.output.coin => {
                record(Step::Ok(format!(
                    "[{scheme}] ReadUtxos: {}:{} re-read matches (coin={})",
                    hex::encode(item.txo_ref.hash),
                    item.txo_ref.index,
                    item.output.coin
                )));
                outcome.reread = Some(reread);
            }
            Ok(Some(reread)) => record(Step::Fail(format!(
                "[{scheme}] ReadUtxos: {}:{} re-read disagrees with SearchUtxos (coin {} vs {})",
                hex::encode(item.txo_ref.hash),
                item.txo_ref.index,
                reread.output.coin,
                item.output.coin
            ))),
            Ok(None) => record(Step::Fail(format!(
                "[{scheme}] ReadUtxos: {}:{} not found, but SearchUtxos just returned it",
                hex::encode(item.txo_ref.hash),
                item.txo_ref.index
            ))),
            Err(e) => record(Step::Fail(format!("[{scheme}] ReadUtxos failed: {e}"))),
        }
    } else {
        record(Step::Warn(format!(
            "[{scheme}] no UTxOs at this hash -- nothing to re-read"
        )));
    }

    let fmt_tx_found = |scheme: Scheme, hash: TxHash, tx: &ReadTxResponse| {
        format!(
            "[{scheme}] ReadTx {}: found, {} input(s) {} output(s), first_output={:?}",
            hex::encode(hash),
            tx.tx.inputs.len(),
            tx.tx.outputs.len(),
            tx.tx.outputs.first().map(precis_output)
        )
    };
    match (cli.tx_hash, derived_tx_hash) {
        (Some(hash), _) => match client.read_tx(ReadTxRequest { hash }).await {
            Ok(Some(tx)) => {
                record(Step::Ok(fmt_tx_found(scheme, hash, &tx)));
                outcome.tx = Some(tx);
            }
            Ok(None) => record(Step::Warn(format!(
                "[{scheme}] ReadTx {}: not found (explicit --tx-hash, not derived)",
                hex::encode(hash)
            ))),
            Err(e) => record(Step::Fail(format!("[{scheme}] ReadTx failed: {e}"))),
        },
        (None, Some(hash)) => match client.read_tx(ReadTxRequest { hash }).await {
            Ok(Some(tx)) => {
                record(Step::Ok(fmt_tx_found(scheme, hash, &tx)));
                outcome.tx = Some(tx);
            }
            Ok(None) => record(Step::Fail(format!(
                "[{scheme}] ReadTx {}: not found, but it created a UTxO SearchUtxos just returned",
                hex::encode(hash)
            ))),
            Err(e) => record(Step::Fail(format!("[{scheme}] ReadTx failed: {e}"))),
        },
        (None, None) => record(Step::Warn(format!(
            "[{scheme}] no tx to check (no --tx-hash, no UTxO found)"
        ))),
    }

    if let Some(hash) = cli.datum_hash {
        match client.read_data(ReadDataRequest { key: hash }).await {
            Ok(Some(datum)) => {
                record(Step::Ok(format!(
                    "[{scheme}] ReadData {}: found, {} byte(s), native_bytes[..8]={}",
                    hex::encode(hash),
                    datum.value.native_bytes.len(),
                    hex::encode(&datum.value.native_bytes[..datum.value.native_bytes.len().min(8)])
                )));
                outcome.datum = Some(datum);
            }
            Ok(None) => record(Step::Warn(format!(
                "[{scheme}] ReadData {}: not found",
                hex::encode(hash)
            ))),
            Err(e) => record(Step::Fail(format!("[{scheme}] ReadData failed: {e}"))),
        }
    }

    if let Some(hash) = stake_hash {
        match client
            .search_utxos(SearchUtxosRequest {
                pay_hash: None,
                stake_hash: Some(hash),
                since_slot: None,
                since_hash: None,
                start_token: None,
                max_items: Some(1),
            })
            .await
        {
            Ok(page) => {
                let first = page.items.first().map(precis_item);
                record(Step::Ok(format!(
                    "[{scheme}] SearchUtxos(stake_hash={}): {} item(s), first={:?}",
                    hex::encode(hash),
                    page.items.len(),
                    first
                )));
                outcome.stake_page = Some(page);
            }
            Err(e) => record(Step::Fail(format!(
                "[{scheme}] SearchUtxos (stake_hash) failed: {e}"
            ))),
        }
    }

    // `since_slot=0`: every row matching the hash from genesis,
    // including anything already spent -- exercises `SearchUtxos`'s
    // `since` addition (the one-shot reorg-safe diff `UtxoChanges` used
    // to be a separate RPC for).
    match client
        .search_utxos(SearchUtxosRequest {
            pay_hash: Some(pay_hash),
            stake_hash: None,
            since_slot: Some(0),
            since_hash: None,
            start_token: None,
            max_items: None,
        })
        .await
    {
        Ok(page) => {
            record(Step::Ok(format!(
                "[{scheme}] SearchUtxos(since_slot=0): {} item(s)",
                page.items.len()
            )));
            outcome.since_page = Some(page);
        }
        Err(e) => record(Step::Fail(format!(
            "[{scheme}] SearchUtxos (since_slot=0) failed: {e}"
        ))),
    }

    // Did `GetMetrics` pick up the requests this run itself just made?
    // A loose lower bound (strictly more, not an exact count) -- robust
    // against unrelated concurrent traffic, but still catches a scheme
    // whose requests never get counted at all.
    if let Some(before) = metrics_before {
        match client.get_metrics().await {
            Ok(after) => {
                let (b, a) = (
                    total_requests(&before, scheme),
                    total_requests(&after, scheme),
                );
                if a > b {
                    record(Step::Ok(format!(
                        "[{scheme}] GetMetrics: requests[{scheme}] {b} -> {a}, reflects this run's own traffic"
                    )));
                } else {
                    record(Step::Fail(format!(
                        "[{scheme}] GetMetrics: requests[{scheme}] didn't increase ({b} -> {a}) despite this run's own traffic"
                    )));
                }
            }
            Err(e) => record(Step::Fail(format!(
                "[{scheme}] GetMetrics (post-run) failed: {e}"
            ))),
        }
    }

    Ok(outcome)
}

/// Connects `scheme` and runs the scheme-agnostic smoke flow on it.
async fn run_scheme(scheme: Scheme, cli: &Cli) -> Result<(AnyClient, SmokeOutcome)> {
    let mut client = floe_client::connect(
        scheme,
        &cli.scheme,
        Duration::from_secs(cli.ready_timeout_secs),
    )
    .await?;
    let outcome = run_smoke(&mut client, cli, scheme).await?;
    Ok((client, outcome))
}

/// Polls `pay_hash`'s `since_slot=0` page (same one-shot reorg-safe
/// diff `run_smoke` itself checks) until `tx_hash`'s output shows up in
/// it -- the `since` mechanism doubling as "has this scheme's view
/// caught up to this tx yet", not just "what changed".
async fn poll_until_visible<B: Backend>(
    client: &mut B,
    scheme: Scheme,
    pay_hash: Hash28,
    tx_hash: TxHash,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        let page = client
            .search_utxos(SearchUtxosRequest {
                pay_hash: Some(pay_hash),
                stake_hash: None,
                since_slot: Some(0),
                since_hash: None,
                start_token: None,
                max_items: None,
            })
            .await?;
        if page.items.iter().any(|u| u.txo_ref.hash == tx_hash) {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            anyhow::bail!(
                "[{scheme}] tx {} not visible within {timeout:?}",
                hex::encode(tx_hash)
            );
        }
        debug!(scheme = %scheme, tx_hash = %hex::encode(tx_hash), "not visible yet, retrying");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Chains every connected scheme's `SubmitTx` into one relay instead
/// of each submitting independently: scheme `i` submits a
/// self-payment spending whatever `pay_hash`'s address currently
/// holds, scheme `i+1` polls (`poll_until_visible`) until that tx is
/// visible to *it*, then submits its own -- so each scheme's submit
/// actually depends on the previous scheme's having been indexed and
/// queryable, not just broadcast. The last scheme wraps back to the
/// first for its own poll (a horseshoe bent shut into a closed loop),
/// closing the chain without an extra submit. One scheme given closes
/// on itself.
///
/// `ready_timeout_secs` also bounds each hop's visibility poll, same
/// budget as the initial per-scheme ready-wait -- a Cardano block can
/// easily take longer than a short fixed window.
async fn submit_relay(
    clients: &mut [(Scheme, AnyClient)],
    skey: [u8; 32],
    ready_timeout_secs: u64,
) -> bool {
    let pay_hash = self_pay::pay_hash(skey);
    let n = clients.len();
    let timeout = Duration::from_secs(ready_timeout_secs);
    let mut ok = true;
    for i in 0..n {
        let scheme_i = clients[i].0;
        let result = self_pay::run(&mut clients[i].1, skey, true).await;
        let tx_hash = match result {
            Ok(self_pay::SelfPayResult::Submitted { r#ref: tx_ref }) => {
                match hex::decode(&tx_ref).ok().and_then(|v| v.try_into().ok()) {
                    Some(h) => h,
                    None => {
                        error!(
                            "[fail] [{scheme_i}] SubmitTx relay: ref isn't a 32-byte hash: {tx_ref}"
                        );
                        ok = false;
                        break;
                    }
                }
            }
            Ok(self_pay::SelfPayResult::Built { .. }) => {
                unreachable!("self_pay::run(.., submit: true) always returns Submitted or Err")
            }
            Err(self_pay::SelfPayError::NoFunds) => {
                warn!("[warn] [{scheme_i}] SubmitTx relay: no funds, stopping the chain here");
                break;
            }
            Err(self_pay::SelfPayError::InsufficientFunds) => {
                warn!(
                    "[warn] [{scheme_i}] SubmitTx relay: best UTxO can't cover fee + margin, stopping the chain here"
                );
                break;
            }
            Err(self_pay::SelfPayError::Other(e)) => {
                error!("[fail] [{scheme_i}] SubmitTx relay failed: {e}");
                ok = false;
                break;
            }
        };
        let relay_i = i + 1;
        info!(
            "[ok]   [{scheme_i}] SubmitTx (relay {relay_i}/{n}): submitted, ref={}",
            hex::encode(tx_hash)
        );

        let next = (i + 1) % n;
        let scheme_next = clients[next].0;
        let closing = next == 0 && i == n - 1;
        match poll_until_visible(
            &mut clients[next].1,
            scheme_next,
            pay_hash,
            tx_hash,
            timeout,
        )
        .await
        {
            Ok(()) => info!(
                "[ok]   [{scheme_next}] tx {} visible{}",
                hex::encode(tx_hash),
                if closing { " (horseshoe closed)" } else { "" }
            ),
            Err(e) => {
                error!("[fail] {e}");
                ok = false;
            }
        }
    }
    ok
}

/// The ledger tip can advance between two schemes' calls; that's not a
/// disagreement.
fn normalize_tip(tip: &mut ChainPoint) {
    *tip = ChainPoint::default();
}

fn fingerprint_page(page: &SearchUtxosResponse) -> String {
    let mut page = page.clone();
    normalize_tip(&mut page.ledger_tip);
    format!("{page:?}")
}

fn fingerprint_tx(tx: &ReadTxResponse) -> String {
    let mut tx = tx.clone();
    normalize_tip(&mut tx.ledger_tip);
    format!("{tx:?}")
}

fn fingerprint_datum(datum: &ReadDataResponse) -> String {
    let mut datum = datum.clone();
    normalize_tip(&mut datum.ledger_tip);
    format!("{datum:?}")
}

/// Compares one field of a baseline scheme's outcome against every
/// other scheme's, one step per field (not per pair).
fn cross_check<T>(
    field: &str,
    baseline: (Scheme, &Option<T>),
    others: &[(Scheme, Option<T>)],
    fingerprint: impl Fn(&T) -> String,
) -> Step {
    let (base_scheme, base_val) = baseline;
    let Some(base_val) = base_val else {
        return Step::Warn(format!(
            "{field}: {base_scheme} has nothing to compare (absent data)"
        ));
    };
    let base_fp = fingerprint(base_val);
    let mut mismatches = Vec::new();
    for (scheme, val) in others {
        match val {
            Some(v) if fingerprint(v) == base_fp => {}
            Some(_) => mismatches.push(format!("{scheme} disagrees with {base_scheme}")),
            None => mismatches.push(format!(
                "{scheme} returned nothing but {base_scheme} didn't"
            )),
        }
    }
    if mismatches.is_empty() {
        Step::Ok(format!(
            "cross-scheme {field}: all {} scheme(s) agree",
            others.len() + 1
        ))
    } else {
        Step::Fail(format!("cross-scheme {field}: {}", mismatches.join("; ")))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    if cli.submit && cli.skey.is_none() {
        anyhow::bail!("--submit requires --skey (or $FLOE_SMOKE_SKEY)");
    }
    let schemes = cli.scheme.selected();
    if schemes.is_empty() {
        anyhow::bail!(
            "need --grpc and/or --http (with --http-json/--http-cbor) to know what to smoke-test"
        );
    }

    let mut outcomes = Vec::with_capacity(schemes.len());
    let mut clients = Vec::with_capacity(schemes.len());
    let mut ok = true;
    for &scheme in &schemes {
        match run_scheme(scheme, &cli).await {
            Ok((client, outcome)) => {
                ok &= outcome.ok;
                outcomes.push((scheme, outcome));
                clients.push((scheme, client));
            }
            Err(e) => {
                error!("[fail] [{scheme}] {e}");
                ok = false;
            }
        }
    }

    if outcomes.len() > 1 {
        let (base_scheme, base) = &outcomes[0];
        let others: Vec<(Scheme, &SmokeOutcome)> =
            outcomes[1..].iter().map(|(s, o)| (*s, o)).collect();

        macro_rules! check {
            ($field:ident, $fp:expr) => {
                let others_field: Vec<(Scheme, Option<_>)> =
                    others.iter().map(|(s, o)| (*s, o.$field.clone())).collect();
                let step = cross_check(
                    stringify!($field),
                    (*base_scheme, &base.$field),
                    &others_field,
                    $fp,
                );
                if matches!(step, Step::Fail(_)) {
                    ok = false;
                }
                report(&step);
            };
        }
        check!(page, fingerprint_page);
        check!(reread, |u: &Utxo| format!("{u:?}"));
        check!(stake_page, fingerprint_page);
        check!(tx, fingerprint_tx);
        check!(datum, fingerprint_datum);
        check!(since_page, fingerprint_page);
    }

    if cli.submit {
        if clients.is_empty() {
            error!("[fail] --submit: no scheme connected, nothing to relay");
            ok = false;
        } else {
            let skey = cli.skey.expect("validated above: --submit requires --skey");
            ok &= submit_relay(&mut clients, skey, cli.ready_timeout_secs).await;
        }
    }

    if ok {
        info!("SMOKE OK");
        Ok(())
    } else {
        error!("SMOKE FAILED");
        std::process::exit(1);
    }
}
