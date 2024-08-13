// Copyright 2024 RisingWave Labs
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

use std::collections::Bound::{Excluded, Unbounded};
use std::collections::{BTreeMap, HashSet};

use risingwave_common::catalog::TableId;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::debug;

use crate::manager::WorkerId;

#[derive(Debug)]
struct CreatingStreamingJobEpochState {
    node_to_collect: HashSet<WorkerId>,
    resps: Vec<BarrierCompleteResponse>,
    upstream_epoch_to_notify: Option<u64>,
    is_checkpoint: bool,
}

#[derive(Debug)]
pub(super) struct CreatingStreamingJobBarrierControl {
    table_id: TableId,
    // key is prev_epoch of barrier
    inflight_barrier_queue: BTreeMap<u64, CreatingStreamingJobEpochState>,
    max_collected_epoch: Option<u64>,
    max_attached_epoch: Option<u64>,
    collected_barrier: Vec<(u64, Vec<BarrierCompleteResponse>)>,
}

impl CreatingStreamingJobBarrierControl {
    pub(super) fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            inflight_barrier_queue: Default::default(),
            max_collected_epoch: None,
            max_attached_epoch: None,
            collected_barrier: vec![],
        }
    }

    pub(super) fn inflight_barrier_count(&self) -> usize {
        self.inflight_barrier_queue.len()
    }

    pub(super) fn is_wait_on_worker(&self, worker_id: WorkerId) -> bool {
        self.inflight_barrier_queue
            .values()
            .any(|state| state.node_to_collect.contains(&worker_id))
    }

    fn latest_epoch(&self) -> Option<u64> {
        self.inflight_barrier_queue
            .last_key_value()
            .map(|(epoch, _)| *epoch)
            .or(self.max_collected_epoch)
    }

    pub(super) fn enqueue_epoch(
        &mut self,
        epoch: u64,
        node_to_collect: HashSet<WorkerId>,
        is_checkpoint: bool,
    ) {
        debug!(
            epoch,
            ?node_to_collect,
            table_id = self.table_id.table_id,
            "creating job enqueue epoch"
        );
        if let Some(latest_epoch) = self.latest_epoch() {
            assert!(epoch > latest_epoch, "{} {}", epoch, latest_epoch);
        }
        if let Some(max_attached_epoch) = self.max_attached_epoch {
            assert!(epoch > max_attached_epoch);
        }
        if node_to_collect.is_empty() && self.inflight_barrier_queue.is_empty() {
            self.add_collected(epoch, vec![]);
        } else {
            self.inflight_barrier_queue.insert(
                epoch,
                CreatingStreamingJobEpochState {
                    node_to_collect,
                    resps: vec![],
                    upstream_epoch_to_notify: None,
                    is_checkpoint,
                },
            );
        }
    }

    pub(super) fn unsttached_epochs(&self) -> impl Iterator<Item = (u64, bool)> + '_ {
        let range_start = if let Some(max_attached_epoch) = self.max_attached_epoch {
            Excluded(max_attached_epoch)
        } else {
            Unbounded
        };
        self.inflight_barrier_queue
            .range((range_start, Unbounded))
            .map(|(epoch, state)| (*epoch, state.is_checkpoint))
    }

    pub(super) fn attach_upstream_epoch(&mut self, epoch: u64, upstream_epoch: u64) {
        debug!(
            epoch,
            upstream_epoch,
            table_id = ?self.table_id.table_id,
            "attach epoch"
        );
        if let Some(max_attached_epoch) = self.max_attached_epoch {
            assert!(epoch > max_attached_epoch);
        }
        self.max_attached_epoch = Some(epoch);
        let epoch_state = self
            .inflight_barrier_queue
            .get_mut(&epoch)
            .expect("should exist");
        assert!(epoch_state.upstream_epoch_to_notify.is_none());
        epoch_state.upstream_epoch_to_notify = Some(upstream_epoch);
    }

    pub(super) fn collect(
        &mut self,
        epoch: u64,
        worker_id: WorkerId,
        resp: BarrierCompleteResponse,
    ) -> Vec<u64> {
        debug!(
            epoch,
            worker_id,
            table_id = self.table_id.table_id,
            "collect barrier from worker"
        );

        let state = self
            .inflight_barrier_queue
            .get_mut(&epoch)
            .expect("should exist");
        assert!(state.node_to_collect.remove(&worker_id));
        state.resps.push(resp);
        let mut upstream_epochs_to_notify = vec![];
        while let Some((_, state)) = self.inflight_barrier_queue.first_key_value()
            && state.node_to_collect.is_empty()
        {
            let (epoch, state) = self.inflight_barrier_queue.pop_first().expect("non-empty");
            self.add_collected(epoch, state.resps);
            if let Some(upstream_epoch_to_notify) = state.upstream_epoch_to_notify {
                upstream_epochs_to_notify.push(upstream_epoch_to_notify)
            }
        }
        upstream_epochs_to_notify
    }

    fn add_collected(&mut self, epoch: u64, resps: Vec<BarrierCompleteResponse>) {
        if let Some((prev_epoch, _)) = self.collected_barrier.last() {
            assert!(*prev_epoch < epoch);
        }
        self.collected_barrier.push((epoch, resps));
        if let Some(max_collected_epoch) = self.max_collected_epoch {
            assert!(epoch > max_collected_epoch);
        }
        self.max_collected_epoch = Some(epoch);
    }

    pub(super) fn into_epoch_ssts(self) -> Vec<(u64, Vec<BarrierCompleteResponse>)> {
        self.collected_barrier
    }

    pub(super) fn max_collected_epoch(&self) -> Option<u64> {
        self.max_collected_epoch
    }
}
