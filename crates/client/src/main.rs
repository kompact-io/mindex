//! floe-client: CLI front end over the `floe_client` library -- parses
//! args, builds whichever scheme's `Client` the flags ask for, calls
//! one generic `dispatch<B: Backend>`, prints the result as JSON. Every
//! subcommand is named after the RPC it calls, with flags matching that
//! RPC's own request DTO fields (`floe_dto_http::query`/`submit`) --
//! the same mapping the HTTP paths and gRPC methods already are.
//!
//! usage:
//! ```text
//! cargo run -p floe-client -- --grpc 127.0.0.1:50051 read-utxos --key <tx_hash_hex>:<ix>
//! cargo run -p floe-client -- --grpc 127.0.0.1:50051 search-utxos --address <bech32_or_hex>
//! cargo run -p floe-client -- --http 127.0.0.1:8080 --http-json search-utxos --pay-hash <hash_hex>
//! cargo run -p floe-client -- --http 127.0.0.1:8080 --http-cbor read-tx --hash <tx_hash_hex>
//! cargo run -p floe-client -- --grpc 127.0.0.1:50051 read-data --key <datum_hash_hex>
//! cargo run -p floe-client -- --grpc 127.0.0.1:50051 submit-tx <tx_cbor_hex>
//! cargo run -p floe-client -- --grpc 127.0.0.1:50051 get-health
//! cargo run -p floe-client -- key-info --skey <ed25519_skey_hex> [--network <network>] [--output-json]
//! ```

use anyhow::Result;
use clap::{Parser, Subcommand};
use pallas_addresses::Address;

use floe_client::{Backend, Hash28, NetworkArg, SchemeArgs, TxHash, parse_hex};
use floe_dto_http::query::{
    ReadDataRequest, ReadDataResponse, ReadTxRequest, ReadTxResponse, ReadUtxosRequest,
    ReadUtxosResponse, SearchUtxosRequest, SearchUtxosResponse, TxoRef,
};

/// `--key`'s value for `read-utxos`: `<tx_hash_hex>:<ix>`, an exact key.
fn parse_txo_ref(s: &str) -> Result<TxoRef, String> {
    let (h, i) = s.split_once(':').ok_or("must be <tx_hash_hex>:<ix>")?;
    Ok(TxoRef {
        hash: parse_hex::<32>(h)?,
        index: i.parse().map_err(|_| "ix must be a number")?,
    })
}

#[derive(Parser)]
#[command(name = "floe-client", about = "Query a running floe server")]
struct Cli {
    #[command(flatten)]
    scheme: SchemeArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Exact UTxOs by reference (`ReadUtxos`), repeatable.
    ReadUtxos {
        #[arg(long = "key", value_parser = parse_txo_ref, required = true)]
        keys: Vec<TxoRef>,
    },
    /// UTxOs matching an address pattern (`SearchUtxos`). `--address`
    /// (bech32 or hex) is decomposed into `--pay-hash`/`--stake-hash`
    /// client-side (`floe_client::shelley_hashes`) -- same as the
    /// server's own storage; give those directly instead to skip the
    /// decomposition. No network check against the server here -- a
    /// hash from the wrong network just won't match anything.
    ///
    /// With neither `--since-slot` nor `--since-hash`, reports the tip
    /// set only. Given, `items` also includes matching rows spent
    /// after that point. `--since-hash` alone is the reorg-safe form
    /// (the server verifies it's still on the canonical chain); giving
    /// both and having them disagree, or giving a hash the server
    /// doesn't recognize, fails the call -- find an earlier
    /// intersection and retry.
    SearchUtxos {
        #[arg(long)]
        address: Option<String>,
        #[arg(long, value_parser = parse_hex::<28>)]
        pay_hash: Option<Hash28>,
        #[arg(long, value_parser = parse_hex::<28>)]
        stake_hash: Option<Hash28>,
        #[arg(long)]
        since_slot: Option<u64>,
        #[arg(long, value_parser = parse_hex::<32>)]
        since_hash: Option<TxHash>,
        /// Resume cursor, taken verbatim from a previous page's `next_token`.
        #[arg(long)]
        start_token: Option<String>,
        /// Max rows in this page (server caps at 1000, default 100).
        #[arg(long)]
        max_items: Option<i32>,
    },
    /// A datum by hash (`ReadData`) -- raw bytes only, no parsed
    /// `PlutusData` (not implemented yet).
    ReadData {
        #[arg(long, value_parser = parse_hex::<32>)]
        key: TxHash,
    },
    /// A transaction by hash, hydrated from indexed UTxOs (`ReadTx`) --
    /// in/outputs only, no certs/mint/fee/collateral/withdrawals/
    /// auxiliary_data/redeemer (see `floe-server`'s `scheme::grpc`).
    ReadTx {
        #[arg(long, value_parser = parse_hex::<32>)]
        hash: TxHash,
    },
    /// Submit a transaction (`SubmitTx`) -- hex-encoded raw tx CBOR.
    SubmitTx { tx_hex: String },
    /// `GetHealth`.
    GetHealth,
    /// `GetMetrics`.
    GetMetrics,
    /// Prints a signing key's payment hash (blake2b-224 of its
    /// ed25519 public key) and the address it derives (enterprise, no
    /// delegation part), under whichever network's bech32 prefix
    /// `--network` picks. Pure local computation, no RPC -- doesn't
    /// need `--grpc`/`--http`.
    KeyInfo {
        /// Raw ed25519 secret key, hex (32 bytes).
        #[arg(long, env = "FLOE_CLIENT_SKEY", value_parser = parse_hex::<32>)]
        skey: [u8; 32],
        /// Network whose bech32 prefix the derived address uses,
        /// case-insensitive (`mainnet`/`testnet`/`preprod`/`preview`).
        #[arg(long, env = "FLOE_CLIENT_NETWORK", default_value = "mainnet")]
        network: NetworkArg,
        /// Print `address=`/`vkh=` as a single JSON object instead of
        /// `key=value` lines.
        #[arg(long)]
        output_json: bool,
    },
}

/// Whichever DTO a `dispatch` arm produced -- the single print call
/// site in `main` serializes whichever variant came back.
#[derive(serde::Serialize)]
#[serde(untagged)]
enum Output {
    ReadUtxos(ReadUtxosResponse),
    SearchUtxos(SearchUtxosResponse),
    ReadData(ReadDataResponse),
    ReadTx(ReadTxResponse),
    SubmitTx(floe_dto_http::submit::SubmitTxResponse),
    Health(floe_dto_http::health::HealthResponse),
    Metrics(floe_dto_http::health::MetricsResponse),
}

/// The one dispatch function, generic over `B: Backend` -- this is
/// what keeps `main` from tripling its match arms per scheme:
/// genericity does the sharing, not an enum + manual delegation. "Not
/// found"/a rejected submit prints its own stderr message and exits
/// directly; only the success path returns, for `main`'s single print
/// call site.
async fn dispatch<B: Backend>(client: &mut B, cmd: Command) -> Result<Output> {
    match cmd {
        Command::ReadUtxos { keys } => Ok(Output::ReadUtxos(
            client.read_utxos(ReadUtxosRequest { keys }).await?,
        )),
        Command::SearchUtxos {
            address,
            pay_hash,
            stake_hash,
            since_slot,
            since_hash,
            start_token,
            max_items,
        } => {
            let (pay_hash, stake_hash) = if let Some(address) = address {
                let addr: Address = address.parse()?;
                floe_client::shelley_hashes(&addr)?
            } else {
                (
                    pay_hash.ok_or_else(|| {
                        anyhow::anyhow!("need --address or --pay-hash/--stake-hash")
                    })?,
                    stake_hash,
                )
            };
            Ok(Output::SearchUtxos(
                client
                    .search_utxos(SearchUtxosRequest {
                        pay_hash: Some(pay_hash),
                        stake_hash,
                        since_slot,
                        since_hash,
                        start_token,
                        max_items,
                    })
                    .await?,
            ))
        }
        Command::ReadData { key } => match client.read_data(ReadDataRequest { key }).await? {
            Some(datum) => Ok(Output::ReadData(datum)),
            None => {
                eprintln!("not found");
                std::process::exit(1);
            }
        },
        Command::ReadTx { hash } => match client.read_tx(ReadTxRequest { hash }).await? {
            Some(tx) => Ok(Output::ReadTx(tx)),
            None => {
                eprintln!("not found");
                std::process::exit(1);
            }
        },
        Command::SubmitTx { tx_hex } => {
            let raw = hex::decode(&tx_hex)?;
            match client.submit_tx(raw).await {
                Ok(resp) => Ok(Output::SubmitTx(resp)),
                Err(e) => {
                    eprintln!("rejected: {e}");
                    std::process::exit(1);
                }
            }
        }
        Command::GetHealth => Ok(Output::Health(client.get_health().await?)),
        Command::GetMetrics => Ok(Output::Metrics(client.get_metrics().await?)),
        Command::KeyInfo { .. } => unreachable!("main() handles KeyInfo before dispatch"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Pure local computation, no server/scheme involved -- handled
    // before any client is constructed, so it works with no
    // `floe-server` running.
    if let Command::KeyInfo {
        skey,
        network,
        output_json,
    } = &cli.command
    {
        let info = floe_client::key_info::key_info(*skey, network.0)?;
        if *output_json {
            println!("{}", serde_json::to_string(&info)?);
        } else {
            println!("address={}", info.address);
            println!("vkh={}", info.vkh);
        }
        return Ok(());
    }

    let scheme = cli.scheme.selected_one()?;
    let mut client = floe_client::connect(scheme, &cli.scheme, std::time::Duration::ZERO).await?;
    let output = dispatch(&mut client, cli.command).await?;

    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
