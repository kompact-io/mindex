//! http_json: the JSON wire format over `http_common`'s shared socket
//! I/O -- `Accept: application/json`, `Content-Type: application/json`,
//! `serde_json`/`floe-dto-http`'s `Serialize`/`Deserialize` derives for
//! both the request and the response.

use anyhow::Result;

use crate::common::Backend;
use crate::http_common;
use crate::json_wire::decode;
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

    async fn fetch<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let (status, body) = http_common::get(&self.addr, path, "application/json").await?;
        decode(status, &body)
    }

    /// POSTs `req` as a JSON body and decodes the response the same
    /// way -- every `/query`/`/submit` RPC's shape (see
    /// `http_common::post`'s own doc).
    async fn request<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp> {
        let body = serde_json::to_vec(req)?;
        let (status, body) = http_common::post(
            &self.addr,
            path,
            &body,
            "application/json",
            "application/json",
        )
        .await?;
        decode(status, &body)
    }

    /// Like `request`, but a 404 is `Ok(None)` instead of `Err` -- for
    /// `read_tx`/`read_data`, whose `Backend` signature reports "not
    /// found" that way.
    async fn request_opt<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Option<Resp>> {
        let body = serde_json::to_vec(req)?;
        let (status, body) = http_common::post(
            &self.addr,
            path,
            &body,
            "application/json",
            "application/json",
        )
        .await?;
        if status == 404 {
            return Ok(None);
        }
        decode(status, &body).map(Some)
    }
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
            "application/json",
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
