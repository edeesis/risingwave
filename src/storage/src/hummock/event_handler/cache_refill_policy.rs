// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::hummock::{group_delta, HummockVersionDelta};
use tokio::sync::{mpsc, Mutex};

use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockResult, TableHolder};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

const REFILL_DATA_FILE_CACHE_CONCURRENCY: usize = 100;
const REFILL_DATA_FILE_CACHE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct CacheRefillPolicyConfig {
    pub sstable_store: SstableStoreRef,
    pub metrics: Arc<CompactorMetrics>,

    pub max_preload_wait_time_mill: u64,

    pub refill_data_file_cache_levels: HashSet<u32>,
}

pub struct CacheRefillPolicy {
    sstable_store: SstableStoreRef,
    metrics: Arc<CompactorMetrics>,

    max_preload_wait_time_mill: u64,

    refill_data_file_cache_levels: HashSet<u32>,

    concurrency: Arc<Concurrency>,
}

impl CacheRefillPolicy {
    pub fn new(config: CacheRefillPolicyConfig) -> Self {
        Self {
            sstable_store: config.sstable_store,
            metrics: config.metrics,

            max_preload_wait_time_mill: config.max_preload_wait_time_mill,

            refill_data_file_cache_levels: config.refill_data_file_cache_levels,

            concurrency: Arc::new(Concurrency::new(REFILL_DATA_FILE_CACHE_CONCURRENCY)),
        }
    }

    pub async fn execute(self: &Arc<Self>, delta: HummockVersionDelta, max_level: u32) {
        if self.max_preload_wait_time_mill > 0 {
            let policy = self.clone();
            let handle = tokio::spawn(async move {
                let timer = policy.metrics.refill_cache_duration.start_timer();
                let mut preload_count = 0;
                let stats = StoreLocalStatistic::default();
                let mut reqs = vec![];
                let mut levels = vec![];
                let mut removed_sst_object_ids = vec![];
                for group_delta in delta.group_deltas.values() {
                    let mut is_bottommost_level = false;
                    for d in &group_delta.group_deltas {
                        if let Some(group_delta::DeltaType::IntraLevel(level_delta)) =
                            d.delta_type.as_ref()
                        {
                            if level_delta.level_idx >= max_level {
                                is_bottommost_level = true;
                                break;
                            }
                            if level_delta.inserted_table_infos.is_empty() {
                                continue;
                            }
                            let mut level_reqs = vec![];
                            for sst in &level_delta.inserted_table_infos {
                                level_reqs.push(policy.sstable_store.sstable_syncable(sst, &stats));
                            }
                            levels.push(level_delta.level_idx);
                            removed_sst_object_ids
                                .push(level_delta.removed_table_object_ids.clone());
                            preload_count += level_delta.inserted_table_infos.len();
                            reqs.push(level_reqs);
                        }
                    }
                    if is_bottommost_level {
                        reqs.pop();
                        levels.pop();
                        removed_sst_object_ids.pop();
                    }
                }
                policy.metrics.preload_io_count.inc_by(preload_count as u64);
                let insert_ssts = try_join_all(reqs.into_iter().map(try_join_all)).await;

                if !levels.is_empty() && policy.sstable_store.cache_refill_filter().is_some() {
                    tokio::spawn({
                        async move {
                            if let Err(e) = Self::refill_data_file_cache(
                                policy,
                                levels,
                                insert_ssts,
                                removed_sst_object_ids,
                            )
                            .await
                            {
                                tracing::warn!("fill data file cache error: {:?}", e);
                            }
                        }
                    });
                }
                timer.observe_duration();
            });
            let _ = tokio::time::timeout(
                Duration::from_millis(self.max_preload_wait_time_mill),
                handle,
            )
            .await;
        }
    }

    async fn refill_data_file_cache(
        self: Arc<Self>,
        sstable_levels: Vec<u32>,
        fetch_meta_results: HummockResult<Vec<Vec<(TableHolder, u64, u64)>>>,
        removed_sst_object_ids: Vec<Vec<u64>>,
    ) -> HummockResult<()> {
        let metas = fetch_meta_results?
            .into_iter()
            .map(|results| results.into_iter().map(|(meta, _, _)| meta).collect_vec())
            .collect_vec();

        let levels = sstable_levels
            .into_iter()
            .zip_eq_fast(metas)
            .zip_eq_fast(removed_sst_object_ids)
            .map(|((t0, t1), t2)| (t0, t1, t2))
            .collect_vec();

        let mut handles = vec![];
        let cache_refill_filter = self.sstable_store.cache_refill_filter().as_ref().unwrap();

        let start = Instant::now();

        for (level, metas, removed_ssts) in &levels {
            let blocks = metas
                .iter()
                .map(|meta| meta.value().block_count())
                .sum::<usize>();

            if !self.refill_data_file_cache_levels.contains(level) {
                self.metrics
                    .refill_data_file_cache_count
                    .with_label_values(&["ignored"])
                    .inc_by(blocks as f64);
                continue;
            }

            if removed_ssts.is_empty() {
                self.metrics
                    .refill_data_file_cache_count
                    .with_label_values(&["skip"])
                    .inc_by(blocks as f64);
                continue;
            }

            let mut refill = false;
            for id in removed_ssts {
                if cache_refill_filter.contains(id) {
                    refill = true;
                    break;
                }
            }

            if refill {
                for meta in metas {
                    for block_index in 0..meta.value().block_count() {
                        let concurrency = self.concurrency.clone();
                        let meta = meta.value().clone();
                        let mut stat = StoreLocalStatistic::default();
                        let sstable_store = self.sstable_store.clone();
                        let metrics = self.metrics.clone();

                        concurrency.acquire().await;
                        if start.elapsed() > REFILL_DATA_FILE_CACHE_TIMEOUT {
                            self.metrics
                                .refill_data_file_cache_count
                                .with_label_values(&["timeout"])
                                .inc_by(blocks as f64);
                            continue;
                        }

                        let future = async move {
                            let res = sstable_store
                                .may_fill_data_file_cache(&meta, block_index, &mut stat)
                                .await;
                            match res {
                                Ok(true) => {
                                    metrics
                                        .refill_data_file_cache_count
                                        .with_label_values(&["admitted"])
                                        .inc();
                                }
                                Ok(false) => {
                                    metrics
                                        .refill_data_file_cache_count
                                        .with_label_values(&["rejected"])
                                        .inc();
                                }
                                _ => {}
                            }
                            concurrency.release();
                            res
                        };
                        let handle = tokio::spawn(future);
                        handles.push(handle);
                    }
                }
            } else {
                self.metrics
                    .refill_data_file_cache_count
                    .with_label_values(&["filtered"])
                    .inc_by(blocks as f64);
            }
        }

        let _ = try_join_all(handles).await;

        Ok(())
    }
}

pub struct CacheRefillFilter<K>
where
    K: Eq + Ord + Debug + Clone,
{
    refresh_interval: Duration,
    inner: RwLock<CacheRefillFilterInner<K>>,
}

struct CacheRefillFilterInner<K>
where
    K: Eq + Ord + Debug + Clone,
{
    last_refresh: Instant,
    layers: VecDeque<RwLock<BTreeSet<K>>>,
}

impl<K> CacheRefillFilter<K>
where
    K: Eq + Ord + Debug + Clone,
{
    pub fn new(layers: usize, refresh_interval: Duration) -> Self {
        assert!(layers > 0);
        let layers = (0..layers)
            .map(|_| BTreeSet::new())
            .map(RwLock::new)
            .collect();
        let inner = CacheRefillFilterInner {
            last_refresh: Instant::now(),
            layers,
        };
        let inner = RwLock::new(inner);
        Self {
            refresh_interval,
            inner,
        }
    }

    pub fn insert(&self, key: K) {
        if let Some(mut inner) = self.inner.try_write() {
            if inner.last_refresh.elapsed() > self.refresh_interval {
                inner.layers.pop_front();
                inner.layers.push_back(RwLock::new(BTreeSet::new()));
                inner.last_refresh = Instant::now();
            }
        }

        let inner = self.inner.read();
        inner.layers.back().unwrap().write().insert(key);
    }

    pub fn contains(&self, key: &K) -> bool {
        let inner = self.inner.read();
        for layer in inner.layers.iter().rev() {
            if layer.read().contains(key) {
                return true;
            }
        }
        false
    }
}

pub struct Concurrency {
    tx: mpsc::UnboundedSender<()>,
    rx: Mutex<mpsc::UnboundedReceiver<()>>,
}

impl Concurrency {
    pub fn new(concurrency: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        for _ in 0..concurrency {
            tx.send(()).unwrap();
        }
        Self {
            tx,
            rx: Mutex::new(rx),
        }
    }

    pub async fn acquire(&self) {
        self.rx.lock().await.recv().await.unwrap();
    }

    pub fn release(&self) {
        self.tx.send(()).unwrap();
    }
}
