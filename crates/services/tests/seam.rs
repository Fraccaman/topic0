//! Chain-agnostic seam proof: a non-EVM mock (8-byte selectors, `supports_reorg=false`)
//! drives the full ingest→decode path through the same services, no EVM code.

use async_trait::async_trait;
use domain::ports::chain_source::{AuxData, ChainSource};
use domain::ports::decoder::Decoder;
use domain::ports::repository::{BlockRepository, EventRepository, RawRecordRepository};
use domain::ports::unit_of_work::{IngestBatch, UnitOfWork};
use futures::stream::{self, BoxStream};
use schema::{ColumnDef, ColumnType, EventSchema};
use shared::{
    AddressBytes, BlockMeta, ChainCaps, ChainId, DecodedEvent, DomainError, EventRow, EventValue,
    Hash, Height, PlanProfile, RawRecord, RecordFilter, RecordIndex, TipLog,
};
use std::sync::{Arc, Mutex};

const CHAIN: ChainId = ChainId(99);

fn rec(height: u64, idx: u64, disc: u8) -> RawRecord {
    RawRecord {
        chain_id: CHAIN,
        height: Height(height),
        block_hash: Hash(vec![height as u8; 32]),
        index: RecordIndex(idx),
        address: AddressBytes(vec![7; 32]), // 32-byte program pubkey (non-EVM)
        selectors: vec![Hash(vec![disc; 8])], // 8-byte discriminator (non-EVM)
        data: vec![1, 2, 3, 4],
        tx_id: Hash(vec![idx as u8; 64]), // 64-byte signature (non-EVM)
        tx_index: 0,
        inner_index: Some(0),
    }
}

// ---- Mock source ----
struct MockSource {
    cost: pricing::FreeNodeCost,
    plan: PlanProfile,
}
#[async_trait]
impl ChainSource for MockSource {
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
        Ok(vec![rec(10, 0, 0xAB), rec(10, 1, 0xAB)])
    }
    async fn fetch_aux(&self, _r: &[RawRecord]) -> Result<AuxData, DomainError> {
        Ok(AuxData {
            block_metas: vec![BlockMeta {
                chain_id: CHAIN,
                height: Height(10),
                hash: Hash(vec![10; 32]),
                parent_hash: Hash(vec![9; 32]),
                time: 1_700_000_000,
            }],
            enrichment: vec![],
        })
    }
    async fn block_hash(&self, _h: Height) -> Result<Option<Hash>, DomainError> {
        Ok(Some(Hash(vec![10; 32])))
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
            supports_reorg: false,
            supports_subscribe: false,
        }
    }
}

// ---- Mock decoder (discriminator → typed row) ----
struct MockDecoder;
impl Decoder for MockDecoder {
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
                table: "mock_event".into(),
                fields: vec![("amount".into(), EventValue::Uint("42".into()))],
            },
        }))
    }
    fn schemas(&self) -> Vec<EventSchema> {
        vec![EventSchema {
            table: "mock_event".into(),
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

// ---- Mock write side ----
struct CapturedUow(Arc<Mutex<Option<IngestBatch>>>);
#[async_trait]
impl UnitOfWork for CapturedUow {
    async fn commit_ingest(&self, batch: IngestBatch) -> Result<(), DomainError> {
        *self.0.lock().unwrap() = Some(batch);
        Ok(())
    }
    async fn rollback_to(
        &self,
        _chain: shared::ChainId,
        _from: shared::Height,
        _tables: &[String],
    ) -> Result<(), DomainError> {
        Ok(())
    }
}

struct MockEventRepo(Arc<Mutex<Vec<(String, usize)>>>);
#[async_trait]
impl EventRepository for MockEventRepo {
    async fn upsert_batch(&self, table: &str, rows: &[EventRow]) -> Result<u64, DomainError> {
        self.0.lock().unwrap().push((table.to_string(), rows.len()));
        Ok(rows.len() as u64)
    }
    async fn delete_from_height(
        &self,
        _t: &str,
        _c: ChainId,
        _f: Height,
    ) -> Result<u64, DomainError> {
        Ok(0)
    }
}

struct MockRawRepo(Vec<RawRecord>);
#[async_trait]
impl RawRecordRepository for MockRawRepo {
    async fn insert_batch(&self, _r: &[RawRecord]) -> Result<u64, DomainError> {
        Ok(0)
    }
    async fn range(
        &self,
        _c: ChainId,
        _f: Height,
        _t: Height,
    ) -> Result<Vec<RawRecord>, DomainError> {
        Ok(self.0.clone())
    }
    async fn delete_from_height(&self, _c: ChainId, _f: Height) -> Result<u64, DomainError> {
        Ok(0)
    }
}

struct MockBlockRepo;
#[async_trait]
impl BlockRepository for MockBlockRepo {
    async fn upsert_batch(&self, _b: &[BlockMeta]) -> Result<u64, DomainError> {
        Ok(0)
    }
    async fn get(&self, _c: ChainId, _h: Height) -> Result<Option<BlockMeta>, DomainError> {
        Ok(None)
    }
    async fn times(
        &self,
        _c: ChainId,
        _f: Height,
        _t: Height,
    ) -> Result<Vec<(Height, i64)>, DomainError> {
        Ok(vec![(Height(10), 1_700_000_000)])
    }
    async fn max_height(&self, _c: ChainId) -> Result<Option<Height>, DomainError> {
        Ok(None)
    }
    async fn delete_from_height(&self, _c: ChainId, _f: Height) -> Result<u64, DomainError> {
        Ok(0)
    }
    async fn calldata(
        &self,
        _c: ChainId,
        _ids: &[shared::Hash],
    ) -> Result<Vec<shared::TxCalldata>, DomainError> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn non_evm_chain_drives_full_pipeline() {
    let source = MockSource {
        cost: pricing::FreeNodeCost::new("mock"),
        plan: PlanProfile::default(),
    };
    let decoder = MockDecoder;
    let filter = decoder.record_filter();

    // Ingest: build an IngestBatch from the mock source's records.
    let captured = Arc::new(Mutex::new(None));
    let ingest =
        services::IngestionService::new(Box::new(source), Box::new(CapturedUow(captured.clone())));
    let out = ingest
        .ingest_range(&filter, Height(10), Height(10), shared::Epoch(0), true)
        .await
        .unwrap();
    assert_eq!(out.records, 2);
    assert_eq!(out.blocks, 1);
    let batch = captured.lock().unwrap().clone().unwrap();
    assert_eq!(batch.raw_records.len(), 2);
    assert_eq!(batch.block_metas.len(), 1);
    assert!(batch.advance_cursor.is_some());

    // Decode: the mock records → typed rows.
    let upserts = Arc::new(Mutex::new(Vec::new()));
    let written = services::decode_range(
        &decoder,
        &MockRawRepo(vec![rec(10, 0, 0xAB), rec(10, 1, 0xAB)]),
        &MockBlockRepo,
        &MockEventRepo(upserts.clone()),
        ChainId(99),
        Height(10),
        Height(10),
    )
    .await
    .unwrap();
    assert_eq!(written, 2);
    assert_eq!(
        *upserts.lock().unwrap(),
        vec![("mock_event".to_string(), 2)]
    );
}
