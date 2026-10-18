//! http_cbor: the CBOR wire format over `http_common`'s shared socket
//! I/O -- `Accept: application/cbor`, `Content-Type: application/cbor`,
//! `minicbor`/`floe-dto-http`'s `Encode`/`Decode` derives for both the
//! request and the response. Mirror image of `http_json.rs`.

use anyhow::{Result, bail};

use crate::common::Backend;
use crate::http_common;
use floe_dto_http::ErrorBody;
use floe_dto_http::health::{HealthResponse, MetricsResponse};
use floe_dto_http::query::{
    ReadDataRequest, ReadDataResponse, ReadTxRequest, ReadTxResponse, ReadUtxosRequest,
    ReadUtxosResponse, SearchUtxosRequest, SearchUtxosResponse,
};
use floe_dto_http::submit::SubmitTxResponse;

pub struct Client {
    addr: String,
}

impl Client {
    pub fn new(server: &str) -> Self {
        Self {
            addr: http_common::strip_scheme(server).to_string(),
        }
    }

    async fn fetch<T: for<'b> minicbor::Decode<'b, ()>>(&self, path: &str) -> Result<T> {
        let (status, body) = http_common::get(&self.addr, path, "application/cbor").await?;
        decode(status, &body)
    }

    /// POSTs `req` as a CBOR body and decodes the response the same
    /// way -- every `/query`/`/submit` RPC's shape (see
    /// `http_common::post`'s own doc).
    async fn request<Req: minicbor::Encode<()>, Resp: for<'b> minicbor::Decode<'b, ()>>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp> {
        let mut body = Vec::new();
        minicbor::encode(req, &mut body)?;
        let (status, body) = http_common::post(
            &self.addr,
            path,
            &body,
            "application/cbor",
            "application/cbor",
        )
        .await?;
        decode(status, &body)
    }

    /// Like `request`, but a 404 is `Ok(None)` instead of `Err` -- see
    /// `http_json::Client::request_opt`'s doc comment for why.
    async fn request_opt<Req: minicbor::Encode<()>, Resp: for<'b> minicbor::Decode<'b, ()>>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Option<Resp>> {
        let mut body = Vec::new();
        minicbor::encode(req, &mut body)?;
        let (status, body) = http_common::post(
            &self.addr,
            path,
            &body,
            "application/cbor",
            "application/cbor",
        )
        .await?;
        if status == 404 {
            return Ok(None);
        }
        decode(status, &body).map(Some)
    }
}

fn decode<T: for<'b> minicbor::Decode<'b, ()>>(status: u16, body: &[u8]) -> Result<T> {
    if !(200..300).contains(&status) {
        let msg = minicbor::decode::<ErrorBody>(body)
            .map(|e| e.error)
            .unwrap_or_else(|_| hex::encode(body));
        bail!("http {status}: {msg}");
    }
    Ok(minicbor::decode(body)?)
}

impl Backend for Client {
    async fn read_utxos(&mut self, req: ReadUtxosRequest) -> Result<ReadUtxosResponse> {
        self.request(floe_dto_http::query::READ_UTXO_PATH, &req)
            .await
    }

    async fn search_utxos(&mut self, req: SearchUtxosRequest) -> Result<SearchUtxosResponse> {
        self.request(floe_dto_http::query::SEARCH_UTXOS_PATH, &req)
            .await
    }

    async fn read_data(&mut self, req: ReadDataRequest) -> Result<Option<ReadDataResponse>> {
        self.request_opt(floe_dto_http::query::READ_DATA_PATH, &req)
            .await
    }

    async fn read_tx(&mut self, req: ReadTxRequest) -> Result<Option<ReadTxResponse>> {
        self.request_opt(floe_dto_http::query::READ_TX_PATH, &req)
            .await
    }

    async fn submit_tx(&mut self, raw: Vec<u8>) -> Result<SubmitTxResponse> {
        let (status, body) = http_common::post(
            &self.addr,
            floe_dto_http::submit::SUBMIT_TX_PATH,
            &raw,
            "application/cbor",
            "application/cbor",
        )
        .await?;
        decode(status, &body)
    }

    async fn get_health(&mut self) -> Result<HealthResponse> {
        self.fetch(http_common::HEALTH_PATH).await
    }

    async fn get_metrics(&mut self) -> Result<MetricsResponse> {
        self.fetch(http_common::METRICS_PATH).await
    }
}
