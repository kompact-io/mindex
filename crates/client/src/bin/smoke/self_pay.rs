//! self-pay: builds a minimal self-payment (one input, one output back
//! to the same address minus a flat fee guess) from a raw ed25519
//! signing key -- finds a live UTxO at the key's own enterprise
//! address, hand-rolls and signs the tx, optionally submits it.
//! Generic over `Backend`, so it works over any compiled scheme.
//! Smoke-test tooling only: no RPC corresponds to "build and sign a
//! tx", so this isn't part of the general client library.

use pallas_addresses::{
    Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
};
use pallas_crypto::hash::Hasher;
use pallas_crypto::key::ed25519::SecretKey;

use floe_client::Backend;
use floe_dto_http::query::SearchUtxosRequest;

pub enum SelfPayError {
    /// No UTxOs at all -- bin exit code 2.
    NoFunds,
    /// Best UTxO can't cover the fee plus a safety margin -- bin exit code 3.
    InsufficientFunds,
    Other(anyhow::Error),
}

impl From<anyhow::Error> for SelfPayError {
    fn from(e: anyhow::Error) -> Self {
        SelfPayError::Other(e)
    }
}

pub enum SelfPayResult {
    #[allow(dead_code)]
    // `run(.., submit: false)` builds but never submits -- unused by smoke today, kept for completeness.
    Built {
        tx_hex: String,
    },
    Submitted {
        r#ref: String,
    },
}

/// `skey`'s own payment hash -- the same derivation `run` uses
/// internally to find its address's live UTxO, exposed so a caller
/// can poll for that hash independently of calling `run` itself (see
/// `submit_relay`'s chain).
pub fn pay_hash(skey: [u8; 32]) -> [u8; 28] {
    let sk = SecretKey::from(skey);
    let vk = sk.public_key();
    let hash = Hasher::<224>::hash(vk.as_ref());
    hash.as_ref().try_into().expect("blake2b-224 is 28 bytes")
}

pub async fn run<B: Backend>(
    client: &mut B,
    skey: [u8; 32],
    submit: bool,
) -> Result<SelfPayResult, SelfPayError> {
    const FLAT_FEE: u64 = 200_000;
    // Headroom above `FLAT_FEE` so the output comfortably clears
    // min-UTxO (not computed exactly -- a flat guess is fine for a
    // smoke-test payment).
    const MIN_BUFFER: u64 = 1_000_000;

    let sk = SecretKey::from(skey);
    let vk = sk.public_key();
    let pay_hash = Hasher::<224>::hash(vk.as_ref());
    // Hardcoded testnet -- the output address, not an input to hash
    // lookup, so it's orthogonal to which scheme/network the server
    // itself runs.
    let address = Address::Shelley(ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::key_hash(pay_hash),
        ShelleyDelegationPart::Null,
    ));
    let address_bytes = address.to_vec();
    let pay_hash: [u8; 28] = pay_hash
        .as_ref()
        .try_into()
        .expect("blake2b-224 is 28 bytes");

    let page = client
        .search_utxos(SearchUtxosRequest {
            pay_hash: Some(pay_hash),
            stake_hash: None,
            since_slot: None,
            since_hash: None,
            start_token: None,
            max_items: Some(1),
        })
        .await?;
    let Some(item) = page.items.first() else {
        return Err(SelfPayError::NoFunds);
    };
    let input_hash = item.txo_ref.hash;
    let input_ix = item.txo_ref.index as u64;
    let input_coin = item.output.coin;

    if input_coin <= FLAT_FEE + MIN_BUFFER {
        return Err(SelfPayError::InsufficientFunds);
    }
    let output_coin = input_coin - FLAT_FEE;

    // transaction_body = {0: [[input_hash, input_ix]], 1: [[address, coin]], 2: fee}
    let mut body = Vec::new();
    let mut e = minicbor::Encoder::new(&mut body);
    e.map(3)
        .and_then(|e| e.u8(0))
        .and_then(|e| e.array(1))
        .and_then(|e| e.array(2))
        .and_then(|e| e.bytes(&input_hash))
        .and_then(|e| e.u64(input_ix))
        .and_then(|e| e.u8(1))
        .and_then(|e| e.array(1))
        .and_then(|e| e.array(2))
        .and_then(|e| e.bytes(&address_bytes))
        .and_then(|e| e.u64(output_coin))
        .and_then(|e| e.u8(2))
        .and_then(|e| e.u64(FLAT_FEE))
        .map_err(|e| anyhow::anyhow!("cbor encode: {e}"))?;

    // The transaction id *is* this hash -- signed directly (no further
    // hashing/prefix), per Cardano's own witness convention.
    let tx_id = Hasher::<256>::hash(&body);
    let signature = sk.sign(tx_id.as_ref());

    // transaction_witness_set = {0: [[vkey, signature]]}
    let mut witness_set = Vec::new();
    let mut e = minicbor::Encoder::new(&mut witness_set);
    e.map(1)
        .and_then(|e| e.u8(0))
        .and_then(|e| e.array(1))
        .and_then(|e| e.array(2))
        .and_then(|e| e.bytes(vk.as_ref()))
        .and_then(|e| e.bytes(signature.as_ref()))
        .map_err(|e| anyhow::anyhow!("cbor encode: {e}"))?;

    // transaction = [transaction_body, transaction_witness_set, true, null]
    let mut tx = vec![0x84u8];
    tx.extend_from_slice(&body);
    tx.extend_from_slice(&witness_set);
    tx.push(0xF5); // true (is_valid)
    tx.push(0xF6); // null (no auxiliary_data)

    if submit {
        let resp = client.submit_tx(tx).await?;
        Ok(SelfPayResult::Submitted {
            r#ref: hex::encode(resp.r#ref),
        })
    } else {
        Ok(SelfPayResult::Built {
            tx_hex: hex::encode(tx),
        })
    }
}
