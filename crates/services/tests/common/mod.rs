//! Configurable hand-written mocks for service-layer tests (no DB/RPC).
#![allow(dead_code)]

use async_trait::async_trait;
use domain::ports::chain_source::{AuxData, ChainSource};
use domain::ports::decoder::Decoder;
use domain::ports::repository::{
    BlockRepository, CursorRepository, EventRepository, RawRecordRepository,
};
use domain::ports::unit_of_work::{IngestBatch, UnitOfWork};
use futures::stream::{self, BoxStream};
use schema::{ColumnDef, ColumnType, EventSchema};
use shared::{
    AddressBytes, BlockMeta, ChainCaps, ChainId, DecodedEvent, DomainError, EventRow, EventValue,
    Hash, Height, PlanProfile, RawRecord, RecordFilter, RecordIndex, TipLog,
};
use std::sync::{Arc, Mutex};

pub const CHAIN: ChainId = ChainId(99);

/// Build a raw record. `disc` is the selector byte (`0xAB` = known, else skipped).
pub fn rec(height: u64, idx: u64, disc: u8) -> RawRecord {
    RawRecord {
        chain_id: CHAIN,
        height: Height(height),
        block_hash: Hash(vec![height as u8; 32]),
        index: RecordIndex(idx),
        address: AddressBytes(vec![7; 32]),
        selectors: vec![Hash(vec![disc; 8])],
        data: vec![1, 2, 3, 4],
        tx_id: Hash(vec![idx as u8; 64]),
        tx_index: 0,
        inner_index: Some(0),
    }
}

pub fn block_meta(height: u64, hash_byte: u8) -> BlockMeta {
    BlockMeta {
        chain_id: CHAIN,
        height: Height(height),
        hash: Hash(vec![hash_byte; 32]),
        parent_hash: Hash(vec![hash_byte.wrapping_sub(1); 32]),
        time: 1_700_000_000,
    }
}

// ---- Configurable chain source ----
pub struct TestSource {
    pub records: Vec<RawRecord>,
    pub aux: AuxData,
    pub canonical_hash: Option<Hash>,
    pub supports_reorg: bool,
    pub fetch_aux_calls: Arc<Mutex<usize>>,
    cost: pricing::FreeNodeCost,
    plan: PlanProfile,
}

impl TestSource {
    pub fn new() -> Self {
        Self {
            records: vec![],
            aux: AuxData::default(),
            canonical_hash: None,
            supports_reorg: true,
            fetch_aux_calls: Arc::new(Mutex::new(0)),
            cost: pricing::FreeNodeCost::new("mock"),
            plan: PlanProfile::default(),
        }
    }
}

#[async_trait]
impl ChainSource for TestSource {
    fn chain_id(&self) -> ChainId {
        CHAIN
    }
    async fn head(&self) -> Result<Height, DomainError> {
        Ok(Height(100))
    }
    async fn fetch_records(
        &self,
        _f: &RecordFilter,
        _from: Height,
        _to: Height,
    ) -> Result<Vec<RawRecord>, DomainError> {
        Ok(self.records.clone())
    }
    async fn fetch_aux(&self, _r: &[RawRecord]) -> Result<AuxData, DomainError> {
        *self.fetch_aux_calls.lock().unwrap() += 1;
        Ok(self.aux.clone())
    }
    async fn block_hash(&self, _h: Height) -> Result<Option<Hash>, DomainError> {
        Ok(self.canonical_hash.clone())
    }
    async fn subscribe(
        &self,
        _f: &RecordFilter,
    ) -> Result<BoxStream<'static, Result<TipLog, DomainError>>, DomainError> {
        Ok(Box::pin(stream::empty()))
    }
    fn cost_model(&self) -> &dyn domain::CostModel {
        &self.cost
    }
    fn plan(&self) -> &PlanProfile {
        &self.plan
    }
    fn caps(&self) -> ChainCaps {
        ChainCaps {
            supports_reorg: self.supports_reorg,
            supports_subscribe: false,
        }
    }
}

// ---- Configurable decoder (selector 0xAB → row, else skip) ----
pub struct TestDecoder {
    pub table: String,
}

impl TestDecoder {
    pub fn new() -> Self {
        Self {
            table: "mock_event".into(),
        }
    }
}

impl Decoder for TestDecoder {
    fn decode(&self, r: &RawRecord) -> Result<Option<EventRow>, DomainError> {
        if r.selectors.first().map(|s| s.0.as_slice()) != Some(&[0xAB; 8]) {
            return Ok(None);
        }
        Ok(Some(EventRow {
            chain_id: r.chain_id,
            height: r.height,
            block_hash: r.block_hash.clone(),
            block_time: None,
            tx_id: r.tx_id.clone(),
            index: r.index,
            event: DecodedEvent {
                table: self.table.clone(),
                fields: vec![("amount".into(), EventValue::Uint("42".into()))],
            },
        }))
    }
    fn schemas(&self) -> Vec<EventSchema> {
        vec![EventSchema {
            table: self.table.clone(),
            event: "Mock".into(),
            topic0: None,
            columns: vec![ColumnDef {
                name: "amount".into(),
                ty: ColumnType::UInt(256),
                indexed_hash: false,
            }],
            indexed_positions: vec![],
            pk_columns: EventSchema::event_pk(),
        }]
    }
    fn record_filter(&self) -> RecordFilter {
        RecordFilter {
            addresses: vec![AddressBytes(vec![7; 32])],
            selectors: vec![Hash(vec![0xAB; 8])],
        }
    }
}

// ---- Capturing write side ----
pub struct CapturedUow(pub Arc<Mutex<Option<IngestBatch>>>);
#[async_trait]
impl UnitOfWork for CapturedUow {
    async fn commit_ingest(&self, batch: IngestBatch) -> Result<(), DomainError> {
        *self.0.lock().unwrap() = Some(batch);
        Ok(())
    }
    async fn rollback_to(
        &self,
        _chain: ChainId,
        _from: Height,
        _tables: &[String],
    ) -> Result<(), DomainError> {
        Ok(())
    }
}

/// Recorded `rollback_to` calls: `(chain, delete-from height, tables)`.
pub type RollbackLog = Arc<Mutex<Vec<(ChainId, Height, Vec<String>)>>>;

/// UnitOfWork that records `rollback_to` calls (reorg tests). `fail` makes the
/// rollback error, so the service-level error path can be asserted.
#[derive(Clone, Default)]
pub struct TestUow {
    pub rollbacks: RollbackLog,
    pub fail: bool,
}
#[async_trait]
impl UnitOfWork for TestUow {
    async fn commit_ingest(&self, _batch: IngestBatch) -> Result<(), DomainError> {
        Ok(())
    }
    async fn rollback_to(
        &self,
        chain: ChainId,
        from: Height,
        tables: &[String],
    ) -> Result<(), DomainError> {
        if self.fail {
            return Err(DomainError::Storage("rollback failed".into()));
        }
        self.rollbacks
            .lock()
            .unwrap()
            .push((chain, from, tables.to_vec()));
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct TestEventRepo {
    pub upserts: Arc<Mutex<Vec<(String, usize)>>>,
    pub deletes: Arc<Mutex<Vec<(String, Height)>>>,
}
#[async_trait]
impl EventRepository for TestEventRepo {
    async fn upsert_batch(&self, table: &str, rows: &[EventRow]) -> Result<u64, DomainError> {
        self.upserts
            .lock()
            .unwrap()
            .push((table.to_string(), rows.len()));
        Ok(rows.len() as u64)
    }
    async fn delete_from_height(
        &self,
        t: &str,
        _c: ChainId,
        f: Height,
    ) -> Result<u64, DomainError> {
        self.deletes.lock().unwrap().push((t.to_string(), f));
        Ok(0)
    }
}

#[derive(Clone, Default)]
pub struct TestRawRepo {
    pub records: Vec<RawRecord>,
    pub deletes: Arc<Mutex<Vec<Height>>>,
}
#[async_trait]
impl RawRecordRepository for TestRawRepo {
    async fn insert_batch(&self, _r: &[RawRecord]) -> Result<u64, DomainError> {
        Ok(0)
    }
    async fn range(
        &self,
        _c: ChainId,
        _f: Height,
        _t: Height,
    ) -> Result<Vec<RawRecord>, DomainError> {
        Ok(self.records.clone())
    }
    async fn delete_from_height(&self, _c: ChainId, f: Height) -> Result<u64, DomainError> {
        self.deletes.lock().unwrap().push(f);
        Ok(0)
    }
}

#[derive(Clone, Default)]
pub struct TestBlockRepo {
    pub max_height: Option<Height>,
    pub stored: Option<BlockMeta>,
    pub times: Vec<(Height, i64)>,
    pub deletes: Arc<Mutex<Vec<Height>>>,
}
#[async_trait]
impl BlockRepository for TestBlockRepo {
    async fn upsert_batch(&self, _b: &[BlockMeta]) -> Result<u64, DomainError> {
        Ok(0)
    }
    async fn get(&self, _c: ChainId, _h: Height) -> Result<Option<BlockMeta>, DomainError> {
        Ok(self.stored.clone())
    }
    async fn times(
        &self,
        _c: ChainId,
        _f: Height,
        _t: Height,
    ) -> Result<Vec<(Height, i64)>, DomainError> {
        Ok(self.times.clone())
    }
    async fn max_height(&self, _c: ChainId) -> Result<Option<Height>, DomainError> {
        Ok(self.max_height)
    }
    async fn delete_from_height(&self, _c: ChainId, f: Height) -> Result<u64, DomainError> {
        self.deletes.lock().unwrap().push(f);
        Ok(0)
    }
    async fn calldata(
        &self,
        _c: ChainId,
        _ids: &[Hash],
    ) -> Result<Vec<shared::TxCalldata>, DomainError> {
        Ok(vec![])
    }
}

#[derive(Clone, Default)]
pub struct TestCursorRepo {
    pub rewinds: Arc<Mutex<Vec<Height>>>,
    pub advances: Arc<Mutex<Vec<(Height, Hash)>>>,
}
#[async_trait]
impl CursorRepository for TestCursorRepo {
    async fn get(&self, _c: ChainId) -> Result<Option<(Height, Hash)>, DomainError> {
        Ok(None)
    }
    async fn advance(&self, _c: ChainId, last: Height, hash: Hash) -> Result<(), DomainError> {
        self.advances.lock().unwrap().push((last, hash));
        Ok(())
    }
    async fn rewind(&self, _c: ChainId, to: Height) -> Result<(), DomainError> {
        self.rewinds.lock().unwrap().push(to);
        Ok(())
    }
}
