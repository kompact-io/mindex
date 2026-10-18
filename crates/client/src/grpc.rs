//! grpc: the gRPC scheme -- `Client` wraps both `QueryServiceClient`
//! and `SubmitServiceClient` (`floe-server` always serves both on the
//! same address, so `connect` eagerly connects both rather than
//! lazily). `impl Backend` is the one place with genuinely new logic
//! in this crate: converting between `floe-dto-grpc`'s prost types and
//! the same `floe-dto-http` DTOs every scheme takes/returns.

use anyhow::Result;
use tonic::transport::Channel;

use floe_dto_grpc::floe::v1::cardano as c;
use floe_dto_grpc::floe::v1::health as h;
use floe_dto_grpc::floe::v1::health::health_service_client::HealthServiceClient;
use floe_dto_grpc::floe::v1::query as q;
use floe_dto_grpc::floe::v1::query::query_service_client::QueryServiceClient;
use floe_dto_grpc::floe::v1::submit::submit_service_client::SubmitServiceClient;
use floe_dto_grpc::floe::v1::submit::{AnyChainTx, SubmitTxRequest, any_chain_tx};
use floe_dto_http::cardano as hc;
use floe_dto_http::health as hh;
use floe_dto_http::query as hq;
use floe_dto_http::submit as hs;

use crate::common::{Backend, TxHash};

pub struct Client {
    query: QueryServiceClient<Channel>,
    submit: SubmitServiceClient<Channel>,
    health: HealthServiceClient<Channel>,
}

impl Client {
    pub async fn connect(server: &str) -> Result<Self> {
        let channel = Channel::from_shared(server.to_string())?.connect().await?;
        Ok(Self {
            query: QueryServiceClient::new(channel.clone()),
            submit: SubmitServiceClient::new(channel.clone()),
            health: HealthServiceClient::new(channel),
        })
    }
}

// --- proto -> DTO conversion, health/metrics --------------------------

fn stage_dto(s: i32) -> hh::Stage {
    match h::Stage::try_from(s).unwrap_or(h::Stage::Unspecified) {
        h::Stage::Unspecified | h::Stage::Seeding => hh::Stage::Seeding,
        h::Stage::Catchup => hh::Stage::Catchup,
        h::Stage::Running => hh::Stage::Running,
    }
}

fn scheme_dto(s: i32) -> hh::Scheme {
    match h::Scheme::try_from(s).unwrap_or(h::Scheme::Unspecified) {
        h::Scheme::Unspecified | h::Scheme::Grpc => hh::Scheme::Grpc,
        h::Scheme::HttpJson => hh::Scheme::HttpJson,
        h::Scheme::HttpCbor => hh::Scheme::HttpCbor,
    }
}

fn health_response_dto(resp: h::HealthResponse) -> Result<hh::HealthResponse> {
    let h::health_response::Mode::Run(r) = resp
        .mode
        .ok_or_else(|| anyhow::anyhow!("HealthResponse has no mode"))?;
    Ok(hh::HealthResponse::Run(hh::RunHealth {
        stage: stage_dto(r.stage),
        tip_slot: r.tip_slot,
        block_no: r.block_no,
        tip_hash: r
            .tip_hash
            .try_into()
            .map_err(|_| anyhow::anyhow!("RunHealth.tip_hash must be 32 bytes"))?,
        peer_tip_block_no: r.peer_tip_block_no,
    }))
}

fn metrics_response_dto(resp: h::MetricsResponse) -> hh::MetricsResponse {
    hh::MetricsResponse {
        applied_total: resp.applied_total,
        rolled_back_total: resp.rolled_back_total,
        utxos_created_total: resp.utxos_created_total,
        utxo_count: resp.utxo_count,
        requests: resp
            .requests
            .into_iter()
            .map(|r| hh::RequestCount {
                scheme: scheme_dto(r.scheme),
                method: r.method,
                count: r.count,
            })
            .collect(),
    }
}

// --- proto -> DTO conversion -------------------------------------------
//
// Mirrors `floe-server`'s own `any_utxo_data`/`any_chain_tx` field for
// field, in reverse -- same source shapes, opposite direction.

fn chain_point_dto(p: Option<q::ChainPoint>) -> hq::ChainPoint {
    match p {
        None => hq::ChainPoint::default(),
        Some(p) => hq::ChainPoint {
            slot: p.slot,
            hash: p.hash,
            height: p.height,
            timestamp: p.timestamp,
        },
    }
}

fn big_int_u64(b: Option<c::BigInt>) -> u64 {
    match b.and_then(|b| b.big_int) {
        Some(c::big_int::BigInt::Int(i)) => i as u64,
        Some(c::big_int::BigInt::BigUInt(bytes)) => {
            let mut buf = [0u8; 8];
            let n = bytes.len().min(8);
            buf[8 - n..].copy_from_slice(&bytes[bytes.len() - n..]);
            u64::from_be_bytes(buf)
        }
        _ => 0,
    }
}

fn assets_dto(assets: Vec<c::Multiasset>) -> Vec<hc::Multiasset> {
    assets
        .into_iter()
        .map(|ma| hc::Multiasset {
            policy_id: ma.policy_id,
            assets: ma
                .assets
                .into_iter()
                .map(|a| hc::Asset {
                    name: a.name,
                    quantity: big_int_u64(a.quantity),
                })
                .collect(),
        })
        .collect()
}

fn datum_dto(d: Option<c::Datum>) -> Result<Option<hc::Datum>> {
    d.map(|d| -> Result<hc::Datum> {
        Ok(hc::Datum {
            hash: d
                .hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("Datum.hash must be 32 bytes"))?,
            original_cbor: d.original_cbor,
        })
    })
    .transpose()
}

/// Plutus scripts carry their raw bytes on the wire (`bytes
/// plutus_v{1,2,3,4}`), round-tripped losslessly into `hc::Script.cbor`.
/// A native script does not: `floe-server`'s `script()` sends the
/// *structured* `NativeScript` tree, not the raw CBOR that produced it
/// (see `floe-dto-grpc`'s `cardano.proto`), and re-encoding that tree
/// wouldn't reproduce the original bytes anyway (a different CBOR
/// encoding is an equally valid encoding of the same tree, but not a
/// byte-identical one) -- `hc::Script` requires raw `cbor`, so a native
/// script's own `ReadTx`/`ReadUtxos`/`SearchUtxos` result through this
/// scheme carries `script: None` instead. `--transport http` doesn't
/// have this gap: the server's http routes send raw script bytes
/// regardless of lang.
fn script_dto(s: Option<c::Script>) -> Option<hc::Script> {
    match s?.script? {
        c::script::Script::Native(_) => None,
        c::script::Script::PlutusV1(cbor) => Some(hc::Script {
            lang: hc::ScriptLang::PlutusV1,
            cbor,
        }),
        c::script::Script::PlutusV2(cbor) => Some(hc::Script {
            lang: hc::ScriptLang::PlutusV2,
            cbor,
        }),
        c::script::Script::PlutusV3(cbor) => Some(hc::Script {
            lang: hc::ScriptLang::PlutusV3,
            cbor,
        }),
        c::script::Script::PlutusV4(cbor) => Some(hc::Script {
            lang: hc::ScriptLang::PlutusV4,
            cbor,
        }),
    }
}

fn address_dto(bytes: &[u8]) -> Result<hq::Address> {
    Ok(hq::Address(pallas_addresses::Address::from_bytes(bytes)?))
}

fn tx_output_dto(out: c::TxOutput) -> Result<hc::TxOutput> {
    Ok(hc::TxOutput {
        address: address_dto(&out.address)?,
        coin: big_int_u64(out.coin),
        assets: assets_dto(out.assets),
        datum: datum_dto(out.datum)?,
        script: script_dto(out.script),
    })
}

fn utxo_dto(item: q::AnyUtxoData) -> Result<hq::Utxo> {
    let txo_ref = item
        .txo_ref
        .ok_or_else(|| anyhow::anyhow!("AnyUtxoData has no txo_ref"))?;
    let tx_hash: TxHash = txo_ref
        .hash
        .try_into()
        .map_err(|_| anyhow::anyhow!("TxoRef.hash must be 32 bytes"))?;
    let q::any_utxo_data::ParsedState::Cardano(out) = item
        .parsed_state
        .ok_or_else(|| anyhow::anyhow!("AnyUtxoData has no parsed_state"))?;
    let spent_by = item
        .spent_by
        .map(|h| -> Result<TxHash> {
            h.try_into()
                .map_err(|_| anyhow::anyhow!("spent_by must be 32 bytes"))
        })
        .transpose()?;
    Ok(hq::Utxo {
        txo_ref: hq::TxoRef {
            hash: tx_hash,
            index: txo_ref.index as u16,
        },
        output: tx_output_dto(out)?,
        block_ref: chain_point_dto(item.block_ref),
        spent_by,
    })
}

fn tx_dto(tx_hash: TxHash, block: Option<q::ChainPoint>, t: c::Tx) -> Result<hq::Tx> {
    let inputs = t
        .inputs
        .into_iter()
        .map(|i| -> Result<hc::TxInput> {
            let prev_hash: TxHash = i
                .tx_hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("TxInput.tx_hash must be 32 bytes"))?;
            let out = i
                .as_output
                .ok_or_else(|| anyhow::anyhow!("TxInput has no as_output"))?;
            Ok(hc::TxInput {
                tx_hash: prev_hash,
                output_index: i.output_index as u16,
                as_output: tx_output_dto(out)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let outputs = t
        .outputs
        .into_iter()
        .map(tx_output_dto)
        .collect::<Result<Vec<_>>>()?;
    Ok(hq::Tx {
        hash: tx_hash,
        block_ref: chain_point_dto(block),
        inputs,
        outputs,
        successful: t.successful,
    })
}

// --- DTO -> proto conversion, requests ----------------------------------
//
// The reverse direction -- a shared `floe_dto_http::query` request to
// proto, so `Backend`'s one request shape per RPC works over gRPC too.

fn txo_ref_to_grpc(r: hq::TxoRef) -> q::TxoRef {
    q::TxoRef {
        hash: r.hash.to_vec(),
        index: r.index as u32,
    }
}

/// `SearchUtxos` only ever supports a single address-pattern predicate
/// (`floe-server`'s own `scheme::grpc::parse_predicate` restriction) --
/// same single-hash shape `hq::SearchUtxosRequest.pay_hash`/
/// `stake_hash` already is.
fn hash_predicate(
    payment_part: Option<Vec<u8>>,
    delegation_part: Option<Vec<u8>>,
) -> q::UtxoPredicate {
    q::UtxoPredicate {
        r#match: Some(q::AnyUtxoPattern {
            utxo_pattern: Some(q::any_utxo_pattern::UtxoPattern::Cardano(
                c::TxOutputPattern {
                    address: Some(c::AddressPattern {
                        exact_address: None,
                        payment_part,
                        delegation_part,
                    }),
                    asset: None,
                },
            )),
        }),
        not: vec![],
        all_of: vec![],
        any_of: vec![],
    }
}

fn search_utxos_request_to_grpc(req: &hq::SearchUtxosRequest) -> q::SearchUtxosRequest {
    let since = if req.since_slot.is_none() && req.since_hash.is_none() {
        None
    } else {
        Some(q::Point {
            slot: req.since_slot,
            hash: req.since_hash.map(|h| h.to_vec()),
        })
    };
    q::SearchUtxosRequest {
        predicate: Some(hash_predicate(
            req.pay_hash.map(|h| h.to_vec()),
            req.stake_hash.map(|h| h.to_vec()),
        )),
        field_mask: None,
        max_items: req.max_items,
        start_token: req.start_token.clone(),
        since,
    }
}

impl Backend for Client {
    async fn read_utxos(&mut self, req: hq::ReadUtxosRequest) -> Result<hq::ReadUtxosResponse> {
        let resp = self
            .query
            .read_utxos(q::ReadUtxosRequest {
                keys: req.keys.into_iter().map(txo_ref_to_grpc).collect(),
                field_mask: None,
            })
            .await?
            .into_inner();
        Ok(hq::ReadUtxosResponse {
            items: resp
                .items
                .into_iter()
                .map(utxo_dto)
                .collect::<Result<_>>()?,
            ledger_tip: chain_point_dto(resp.ledger_tip),
        })
    }

    async fn search_utxos(
        &mut self,
        req: hq::SearchUtxosRequest,
    ) -> Result<hq::SearchUtxosResponse> {
        let resp = self
            .query
            .search_utxos(search_utxos_request_to_grpc(&req))
            .await?
            .into_inner();
        Ok(hq::SearchUtxosResponse {
            items: resp
                .items
                .into_iter()
                .map(utxo_dto)
                .collect::<Result<_>>()?,
            ledger_tip: chain_point_dto(resp.ledger_tip),
            next_token: resp.next_token,
        })
    }

    async fn read_data(
        &mut self,
        req: hq::ReadDataRequest,
    ) -> Result<Option<hq::ReadDataResponse>> {
        match self
            .query
            .read_data(q::ReadDataRequest {
                key: req.key.to_vec(),
                field_mask: None,
            })
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                let Some(v) = resp.value else { return Ok(None) };
                Ok(Some(hq::ReadDataResponse {
                    value: hq::AnyChainDatum {
                        native_bytes: v.native_bytes,
                        key: v
                            .key
                            .try_into()
                            .map_err(|_| anyhow::anyhow!("AnyChainDatum.key must be 32 bytes"))?,
                    },
                    ledger_tip: chain_point_dto(resp.ledger_tip),
                }))
            }
            Err(e) if e.code() == tonic::Code::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn read_tx(&mut self, req: hq::ReadTxRequest) -> Result<Option<hq::ReadTxResponse>> {
        match self
            .query
            .read_tx(q::ReadTxRequest {
                hash: req.hash.to_vec(),
                field_mask: None,
            })
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                let Some(tx) = resp.tx else { return Ok(None) };
                let q::any_chain_tx::Chain::Cardano(t) = tx
                    .chain
                    .ok_or_else(|| anyhow::anyhow!("AnyChainTx has no chain"))?;
                Ok(Some(hq::ReadTxResponse {
                    tx: tx_dto(req.hash, tx.block_ref, t)?,
                    ledger_tip: chain_point_dto(resp.ledger_tip),
                }))
            }
            Err(e) if e.code() == tonic::Code::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn submit_tx(&mut self, raw: Vec<u8>) -> Result<hs::SubmitTxResponse> {
        let resp = self
            .submit
            .submit_tx(SubmitTxRequest {
                tx: Some(AnyChainTx {
                    r#type: Some(any_chain_tx::Type::Raw(raw)),
                }),
            })
            .await?
            .into_inner();
        Ok(hs::SubmitTxResponse { r#ref: resp.r#ref })
    }

    async fn get_health(&mut self) -> Result<hh::HealthResponse> {
        let resp = self.health.get_health(()).await?.into_inner();
        health_response_dto(resp)
    }

    async fn get_metrics(&mut self) -> Result<hh::MetricsResponse> {
        let resp = self.health.get_metrics(()).await?.into_inner();
        Ok(metrics_response_dto(resp))
    }
}
