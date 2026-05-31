//! IngestionService — fetch wiring, aux-skip on empty, cursor/enqueue batch shape.

mod common;
use common::*;
use domain::ports::chain_source::AuxData;
use services::IngestionService;
use shared::{Epoch, Height, WorkKind};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn non_empty_records_fetch_aux_and_enqueue() {
    let mut source = TestSource::new();
    source.records = vec![rec(10, 0, 0xAB), rec(10, 1, 0xAB)];
    source.aux = AuxData {
        // last_hash comes from the highest height (12).
        block_metas: vec![block_meta(10, 0x0A), block_meta(12, 0x0C)],
        enrichment: vec![],
    };
    let aux_calls = source.fetch_aux_calls.clone();

    let captured = Arc::new(Mutex::new(None));
    let svc = IngestionService::new(Box::new(source), Box::new(CapturedUow(captured.clone())));
    let decoder = TestDecoder::new();
    let filter = {
        use domain::ports::decoder::Decoder;
        decoder.record_filter()
    };

    let out = svc
        .ingest_range(&filter, Height(0), Height(20), Epoch(7), true)
        .await
        .unwrap();
    assert_eq!(out.records, 2);
    assert_eq!(out.blocks, 2);
    assert_eq!(*aux_calls.lock().unwrap(), 1);

    let batch = captured.lock().unwrap().clone().unwrap();
    let item = batch.enqueue.expect("non-empty records enqueue work");
    assert!(matches!(item.kind, WorkKind::Backfill));
    assert_eq!(item.from, Height(0));
    assert_eq!(item.to, Height(20));
    assert_eq!(item.epoch, Epoch(7));

    let (chain, last, hash) = batch.advance_cursor.expect("cursor advances");
    assert_eq!(chain, CHAIN);
    assert_eq!(last, Height(20));
    assert_eq!(hash, shared::Hash(vec![0x0C; 32])); // highest block's hash
}

#[tokio::test]
async fn empty_records_skip_aux_and_enqueue() {
    let source = TestSource::new(); // records empty by default
    let aux_calls = source.fetch_aux_calls.clone();

    let captured = Arc::new(Mutex::new(None));
    let svc = IngestionService::new(Box::new(source), Box::new(CapturedUow(captured.clone())));
    let decoder = TestDecoder::new();
    let filter = {
        use domain::ports::decoder::Decoder;
        decoder.record_filter()
    };

    let out = svc
        .ingest_range(&filter, Height(0), Height(20), Epoch(0), true)
        .await
        .unwrap();
    assert_eq!(out.records, 0);
    assert_eq!(out.blocks, 0);
    // fetch_aux is NOT called when there are no records.
    assert_eq!(*aux_calls.lock().unwrap(), 0);

    let batch = captured.lock().unwrap().clone().unwrap();
    assert!(batch.enqueue.is_none());
    // cursor still advances to `to`, with an empty hash (no blocks fetched).
    let (_, last, hash) = batch
        .advance_cursor
        .expect("cursor advances even when empty");
    assert_eq!(last, Height(20));
    assert_eq!(hash, shared::Hash(Vec::new()));
}
