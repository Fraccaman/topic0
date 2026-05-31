//! decode_range — skip unknown selectors, join block_time, fan out per table.

mod common;
use common::*;
use services::decode_range;
use shared::Height;

#[tokio::test]
async fn decodes_known_skips_unknown_and_joins_time() {
    let events = TestEventRepo::default();
    let raw = TestRawRepo {
        // 0xAB known (×2), 0x00 unknown (skipped).
        records: vec![rec(10, 0, 0xAB), rec(10, 1, 0xAB), rec(10, 2, 0x00)],
        ..Default::default()
    };
    let blocks = TestBlockRepo {
        times: vec![(Height(10), 1_700_000_000)],
        ..Default::default()
    };

    let written = decode_range(
        &TestDecoder::new(),
        &raw,
        &blocks,
        &events,
        CHAIN,
        Height(0),
        Height(20),
    )
    .await
    .unwrap();

    assert_eq!(written, 2);
    assert_eq!(
        *events.upserts.lock().unwrap(),
        vec![("mock_event".to_string(), 2)]
    );
}

#[tokio::test]
async fn missing_block_time_is_none_not_error() {
    let events = TestEventRepo::default();
    let raw = TestRawRepo {
        records: vec![rec(10, 0, 0xAB)],
        ..Default::default()
    };
    // No times entry for height 10 → block_time stays None, decode still succeeds.
    let blocks = TestBlockRepo::default();

    let written = decode_range(
        &TestDecoder::new(),
        &raw,
        &blocks,
        &events,
        CHAIN,
        Height(0),
        Height(20),
    )
    .await
    .unwrap();
    assert_eq!(written, 1);
}

#[tokio::test]
async fn no_records_writes_nothing() {
    let events = TestEventRepo::default();
    let written = decode_range(
        &TestDecoder::new(),
        &TestRawRepo::default(),
        &TestBlockRepo::default(),
        &events,
        CHAIN,
        Height(0),
        Height(20),
    )
    .await
    .unwrap();
    assert_eq!(written, 0);
    assert!(events.upserts.lock().unwrap().is_empty());
}
