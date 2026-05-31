//! ReorgService — detection + rollback-window arithmetic, driven by mocks.

mod common;
use common::*;
use services::ReorgService;
use shared::Height;

fn service(blocks: TestBlockRepo, uow: TestUow, tables: Vec<String>) -> ReorgService {
    ReorgService::new(Box::new(blocks), Box::new(uow), tables)
}

#[tokio::test]
async fn no_reorg_when_source_unsupported() {
    let mut source = TestSource::new();
    source.supports_reorg = false;
    let svc = service(
        TestBlockRepo {
            max_height: Some(Height(50)),
            stored: Some(block_meta(50, 0x50)),
            ..Default::default()
        },
        TestUow::default(),
        vec!["evt".into()],
    );
    assert_eq!(svc.check(&source, 12).await.unwrap(), None);
}

#[tokio::test]
async fn no_reorg_when_no_stored_tip() {
    let source = TestSource::new();
    let svc = service(
        TestBlockRepo::default(), // max_height = None
        TestUow::default(),
        vec!["evt".into()],
    );
    assert_eq!(svc.check(&source, 12).await.unwrap(), None);
}

#[tokio::test]
async fn no_reorg_when_source_hash_unknown() {
    let mut source = TestSource::new();
    source.canonical_hash = None;
    let svc = service(
        TestBlockRepo {
            max_height: Some(Height(50)),
            stored: Some(block_meta(50, 0x50)),
            ..Default::default()
        },
        TestUow::default(),
        vec!["evt".into()],
    );
    assert_eq!(svc.check(&source, 12).await.unwrap(), None);
}

#[tokio::test]
async fn no_reorg_when_hashes_match() {
    let mut source = TestSource::new();
    source.canonical_hash = Some(shared::Hash(vec![0x50; 32]));
    let uow = TestUow::default();
    let svc = service(
        TestBlockRepo {
            max_height: Some(Height(50)),
            stored: Some(block_meta(50, 0x50)), // hash = [0x50;32] matches canonical
            ..Default::default()
        },
        uow.clone(),
        vec!["evt".into()],
    );
    assert_eq!(svc.check(&source, 12).await.unwrap(), None);
    assert!(uow.rollbacks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn reorg_rolls_back_window_on_hash_mismatch() {
    let mut source = TestSource::new();
    source.canonical_hash = Some(shared::Hash(vec![0xFF; 32])); // != stored [0x50;32]

    let blocks = TestBlockRepo {
        max_height: Some(Height(50)),
        stored: Some(block_meta(50, 0x50)),
        ..Default::default()
    };
    let uow = TestUow::default();
    let svc = service(
        blocks.clone(),
        uow.clone(),
        vec!["evt_a".into(), "evt_b".into()],
    );

    // tip=50, confirmations=12 → fork=38, reindex/delete from 39.
    let from = svc.check(&source, 12).await.unwrap();
    assert_eq!(from, Some(Height(39)));

    // One atomic rollback for the whole window: delete-from `from` over every table,
    // cursor rewind to `fork` handled inside the UnitOfWork.
    assert_eq!(
        *uow.rollbacks.lock().unwrap(),
        vec![(
            shared::ChainId(99),
            Height(39),
            vec!["evt_a".to_string(), "evt_b".to_string()]
        )]
    );
}

#[tokio::test]
async fn fork_saturates_at_zero_for_shallow_chains() {
    let mut source = TestSource::new();
    source.canonical_hash = Some(shared::Hash(vec![0xFF; 32]));
    let uow = TestUow::default();
    let svc = service(
        TestBlockRepo {
            max_height: Some(Height(5)),
            stored: Some(block_meta(5, 0x05)),
            ..Default::default()
        },
        uow.clone(),
        vec!["evt".into()],
    );
    // tip=5, confirmations=12 → fork saturates to 0, reindex from 1.
    assert_eq!(svc.check(&source, 12).await.unwrap(), Some(Height(1)));
    assert_eq!(
        *uow.rollbacks.lock().unwrap(),
        vec![(shared::ChainId(99), Height(1), vec!["evt".to_string()])]
    );
}

#[tokio::test]
async fn reorg_propagates_rollback_failure() {
    let mut source = TestSource::new();
    source.canonical_hash = Some(shared::Hash(vec![0xFF; 32]));
    let svc = service(
        TestBlockRepo {
            max_height: Some(Height(50)),
            stored: Some(block_meta(50, 0x50)),
            ..Default::default()
        },
        TestUow {
            fail: true,
            ..Default::default()
        },
        vec!["evt".into()],
    );
    // A failed (rolled-back) atomic rollback surfaces as an error, not silent success.
    assert!(svc.check(&source, 12).await.is_err());
}
