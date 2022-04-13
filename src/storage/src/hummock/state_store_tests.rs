// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_rpc_client::HummockMetaClient;

use super::{HummockStateStoreIter, HummockStorage, StateStore};
use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
use crate::hummock::key::Epoch;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::test_utils::default_config_for_test;
use crate::monitor::StateStoreMetrics;
use crate::object::{InMemObjectStore, ObjectStoreImpl};
use crate::storage_value::{StorageValue, VALUE_META_SIZE};
use crate::StateStoreIter;

#[tokio::test]
async fn test_basic() {
    let object_client = Arc::new(ObjectStoreImpl::Mem(InMemObjectStore::new()));
    let sstable_store = mock_sstable_store_with_object_store(object_client.clone());
    let hummock_options = Arc::new(default_config_for_test());
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));
    let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        local_version_manager,
        meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), StorageValue::new_default_put("111")),
        (Bytes::from("bb"), StorageValue::new_default_put("222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), StorageValue::new_default_put("333")),
        (anchor.clone(), StorageValue::new_default_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Third batch deletes the anchor
    let mut batch3 = vec![
        (Bytes::from("dd"), StorageValue::new_default_put("444")),
        (Bytes::from("ee"), StorageValue::new_default_put("555")),
        (anchor.clone(), StorageValue::new_default_delete()),
    ];

    // Make sure the batch is sorted.
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // Write the first batch.
    hummock_storage.ingest_batch(batch1, epoch1).await.unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111"));
    let value = hummock_storage
        .get(&Bytes::from("bb"), epoch1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(&Bytes::from("ab"), epoch1)
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write the second batch.
    let epoch2 = epoch1 + 1;
    hummock_storage.ingest_batch(batch2, epoch2).await.unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write the third batch.
    let epoch3 = epoch2 + 1;
    hummock_storage.ingest_batch(batch3, epoch3).await.unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch3).await.unwrap();
    assert_eq!(value, None);

    // Get non-existent maximum key.
    let value = hummock_storage
        .get(&Bytes::from("ff"), epoch3)
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write aa bb
    let mut iter = hummock_storage
        .iter(..=b"ee".to_vec(), epoch1)
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let mut iter = hummock_storage
        .iter(..=b"ee".to_vec(), epoch2)
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);

    // Delete aa, write dd,ee
    let mut iter = hummock_storage
        .iter(..=b"ee".to_vec(), epoch3)
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 4);
    hummock_storage.sync(Some(epoch1)).await.unwrap();
    meta_client.commit_epoch(epoch1).await.unwrap();
    hummock_storage.wait_epoch(epoch1).await.unwrap();
    let value = hummock_storage
        .get(&Bytes::from("bb"), epoch2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));
    let value = hummock_storage
        .get(&Bytes::from("dd"), epoch2)
        .await
        .unwrap();
    assert!(value.is_none());
}

#[tokio::test]
async fn test_state_store_sync() {
    let object_client = Arc::new(ObjectStoreImpl::Mem(InMemObjectStore::new()));
    let sstable_store = mock_sstable_store_with_object_store(object_client.clone());

    let mut config = default_config_for_test();
    config.shared_buffer_threshold_size = 64;
    config.write_conflict_detection_enabled = false;

    let hummock_options = Arc::new(config);
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));
    let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        local_version_manager,
        meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    let mut epoch: Epoch = 1;

    // ingest 16B batch
    let mut batch1 = vec![
        (Bytes::from("aaaa"), StorageValue::new_default_put("1111")),
        (Bytes::from("bbbb"), StorageValue::new_default_put("2222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage.ingest_batch(batch1, epoch).await.unwrap();

    // check sync state store metrics
    // Note: epoch(8B) and ValueMeta(2B) will be appended to each kv pair
    assert_eq!(
        (16 + (8 + VALUE_META_SIZE) * 2) as u64,
        hummock_storage
            .shared_buffer_manager()
            .stats()
            .shared_buffer_cur_size
            .load(Ordering::SeqCst)
    );

    // ingest 24B batch
    let mut batch2 = vec![
        (Bytes::from("cccc"), StorageValue::new_default_put("3333")),
        (Bytes::from("dddd"), StorageValue::new_default_put("4444")),
        (Bytes::from("eeee"), StorageValue::new_default_put("5555")),
    ];
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage.ingest_batch(batch2, epoch).await.unwrap();

    // shared buffer threshold size should have been reached and will trigger a flush
    // then ingest the batch
    assert_eq!(
        (24 + (8 + VALUE_META_SIZE) * 3) as u64,
        hummock_storage
            .shared_buffer_manager()
            .stats()
            .shared_buffer_cur_size
            .load(Ordering::SeqCst)
    );

    epoch += 1;

    // ingest more 8B then will trigger a sync behind the scene
    let mut batch3 = vec![(Bytes::from("eeee"), StorageValue::new_default_put("5555"))];
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage.ingest_batch(batch3, epoch).await.unwrap();

    // 16B in total with 8B epoch appended to the key
    assert_eq!(
        (16 + VALUE_META_SIZE) as u64,
        hummock_storage
            .shared_buffer_manager()
            .stats()
            .shared_buffer_cur_size
            .load(Ordering::SeqCst)
    );

    // triger a sync
    hummock_storage.sync(Some(epoch)).await.unwrap();

    assert_eq!(
        0,
        hummock_storage
            .shared_buffer_manager()
            .stats()
            .shared_buffer_cur_size
            .load(Ordering::SeqCst)
    );
}

async fn count_iter(iter: &mut HummockStateStoreIter<'_>) -> usize {
    let mut c: usize = 0;
    while iter.next().await.unwrap().is_some() {
        c += 1
    }
    c
}

#[tokio::test]
/// Fix this when we finished epoch management.
#[ignore]
async fn test_reload_storage() {
    let object_store = Arc::new(ObjectStoreImpl::Mem(InMemObjectStore::new()));
    let sstable_store = mock_sstable_store_with_object_store(object_store.clone());
    let hummock_options = Arc::new(default_config_for_test());
    let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store.clone(),
        local_version_manager.clone(),
        hummock_meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), StorageValue::new_default_put("111")),
        (Bytes::from("bb"), StorageValue::new_default_put("222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), StorageValue::new_default_put("333")),
        (anchor.clone(), StorageValue::new_default_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // Write the first batch.
    hummock_storage.ingest_batch(batch1, epoch1).await.unwrap();

    // Mock something happened to storage internal, and storage is reloaded.
    drop(hummock_storage);
    let hummock_storage = HummockStorage::with_default_stats(
        Arc::new(default_config_for_test()),
        sstable_store,
        local_version_manager,
        hummock_meta_client,
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(&Bytes::from("ab"), epoch1)
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write the second batch.
    let epoch2 = epoch1 + 1;
    hummock_storage.ingest_batch(batch2, epoch2).await.unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write aa bb
    let mut iter = hummock_storage
        .iter(..=b"ee".to_vec(), epoch1)
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let mut iter = hummock_storage
        .iter(..=b"ee".to_vec(), epoch2)
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);
}
