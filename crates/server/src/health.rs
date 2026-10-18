//! health: core, scheme-independent readiness state -- the writer
//! process's seed/catchup/running lifecycle plus chain-freshness,
//! in-memory. Zero knowledge of HTTP, gRPC, or wire format --
//! `scheme::grpc::health`/`scheme::http::metrics` only snapshot and
//! render this.
//!
//! `Stage` mirrors `store::Mode` (plus `Seeding`, which precedes `Mode`
//! meaning anything) -- it's the storage engine's own lifecycle, not a
//! query-readiness signal: `Mode::Running` switches at `depth <=
//! k+margin`, a reorg-safety threshold reached long before the live
//! tip. `is_ready` is the separate, stricter signal query handlers
//! actually gate on (`Ctx::require_ready`): `Stage::Running` *and*
//! within `fresh`'s hysteresis of tip. Callers that get `NotReady` can
//! still read `block_no`/`peer_tip_block_no` off `/health` to see how
//! far behind that is.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use floe_dto_http::health::RunHealth;
pub use floe_dto_http::health::Stage;

use crate::ingest::decode::TxHash;
use crate::ingest::store::Mode;

impl From<Mode> for Stage {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::Catchup => Stage::Catchup,
            Mode::Running => Stage::Running,
        }
    }
}

/// Hysteresis for `fresh`: enter at `FRESH_ENTER_DEPTH`, only fall
/// back out past `FRESH_EXIT_DEPTH`. Depth-based, not time-based --
/// Cardano can go several minutes between blocks with nothing wrong,
/// and that alone must never look like falling behind.
const FRESH_ENTER_DEPTH: u64 = 2;
const FRESH_EXIT_DEPTH: u64 = 10;

#[derive(Debug, Default)]
pub struct Health {
    magic: AtomicU64,
    stage: AtomicU8,
    fresh: AtomicBool,
    tip_slot: AtomicU64,
    block_no: AtomicU64,
    peer_tip_block_no: AtomicU64,
    applied: AtomicU64,
    rolled_back: AtomicU64,
    utxos_created: AtomicU64,
    /// Not an atomic: a 32-byte hash, written once per applied block --
    /// nowhere near hot enough to need lock-free.
    tip_hash: Mutex<TxHash>,
}

impl Health {
    pub fn set_stage(&self, stage: Stage) {
        self.stage.store(stage as u8, Ordering::Relaxed);
    }

    pub fn set_magic(&self, magic: u64) {
        self.magic.store(magic, Ordering::Relaxed);
    }

    pub fn record_apply(
        &self,
        mode: Mode,
        slot: u64,
        block_no: u64,
        hash: TxHash,
        peer_tip_block_no: u64,
        utxos_created: u64,
    ) {
        self.set_stage(mode.into());
        let threshold = if self.fresh.load(Ordering::Relaxed) {
            FRESH_EXIT_DEPTH
        } else {
            FRESH_ENTER_DEPTH
        };
        let depth = peer_tip_block_no.saturating_sub(block_no);
        self.fresh.store(
            mode == Mode::Running && depth <= threshold,
            Ordering::Relaxed,
        );
        self.tip_slot.store(slot, Ordering::Relaxed);
        self.block_no.store(block_no, Ordering::Relaxed);
        self.peer_tip_block_no
            .store(peer_tip_block_no, Ordering::Relaxed);
        self.applied.fetch_add(1, Ordering::Relaxed);
        self.utxos_created
            .fetch_add(utxos_created, Ordering::Relaxed);
        *self.tip_hash.lock().unwrap() = hash;
    }

    pub fn record_rollback(&self) {
        self.rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stage(&self) -> Stage {
        match self.stage.load(Ordering::Relaxed) {
            0 => Stage::Seeding,
            1 => Stage::Catchup,
            _ => Stage::Running,
        }
    }

    /// `Stage::Running` and within `fresh`'s hysteresis of the peer tip
    /// -- what query handlers gate on, stricter than `stage().is_ready()`.
    pub fn is_ready(&self) -> bool {
        self.stage().is_ready() && self.fresh.load(Ordering::Relaxed)
    }

    pub fn applied_total(&self) -> u64 {
        self.applied.load(Ordering::Relaxed)
    }

    pub fn rolled_back_total(&self) -> u64 {
        self.rolled_back.load(Ordering::Relaxed)
    }

    pub fn utxos_created_total(&self) -> u64 {
        self.utxos_created.load(Ordering::Relaxed)
    }

    /// `/health`/`GetHealth` payload.
    pub fn snapshot(&self) -> RunHealth {
        RunHealth {
            stage: self.stage(),
            tip_slot: self.tip_slot.load(Ordering::Relaxed),
            block_no: self.block_no.load(Ordering::Relaxed),
            tip_hash: *self.tip_hash.lock().unwrap(),
            peer_tip_block_no: self.peer_tip_block_no.load(Ordering::Relaxed),
        }
    }
}
