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
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::mem::take;

use risingwave_common::catalog::TableId;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::debug;

use crate::manager::WorkerId;

#[derive(Debug)]
struct CreatingStreamingJobEpochState {
    epoch: u64,
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
    initial_epoch: Option<u64>,
    max_collected_epoch: Option<u64>,
    max_attached_epoch: Option<u64>,
    // newer epoch at the front. should all be checkpoint barrier
    pending_barriers_to_complete: VecDeque<CreatingStreamingJobEpochState>,
    completing_barrier: Option<CreatingStreamingJobEpochState>,
}

impl CreatingStreamingJobBarrierControl {
    pub(super) fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            inflight_barrier_queue: Default::default(),
            initial_epoch: None,
            max_collected_epoch: None,
            max_attached_epoch: None,
            pending_barriers_to_complete: Default::default(),
            completing_barrier: None,
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

    pub(super) fn max_collected_epoch(&self) -> Option<u64> {
        self.max_collected_epoch
    }

    pub(super) fn is_empty(&self) -> bool {
        self.inflight_barrier_queue.is_empty()
            && self.pending_barriers_to_complete.is_empty()
            && self.completing_barrier.is_none()
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
        if self.initial_epoch.is_none() {
            self.initial_epoch = Some(epoch);
            assert!(is_checkpoint, "first barrier must be checkpoint barrier");
        }
        if let Some(latest_epoch) = self.latest_epoch() {
            assert!(epoch > latest_epoch, "{} {}", epoch, latest_epoch);
        }
        if let Some(max_attached_epoch) = self.max_attached_epoch {
            assert!(epoch > max_attached_epoch);
        }
        let epoch_state = CreatingStreamingJobEpochState {
            epoch,
            node_to_collect,
            resps: vec![],
            upstream_epoch_to_notify: None,
            is_checkpoint,
        };
        if epoch_state.node_to_collect.is_empty() && self.inflight_barrier_queue.is_empty() {
            self.add_collected(epoch_state);
        } else {
            self.inflight_barrier_queue.insert(epoch, epoch_state);
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
    ) {
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
        while let Some((_, state)) = self.inflight_barrier_queue.first_key_value()
            && state.node_to_collect.is_empty()
        {
            let (_, state) = self.inflight_barrier_queue.pop_first().expect("non-empty");
            self.add_collected(state);
        }
    }

    #[expect(clippy::type_complexity)]
    pub(super) fn start_completing(
        &mut self,
    ) -> (Vec<u64>, Option<(u64, Vec<BarrierCompleteResponse>, bool)>) {
        if self.completing_barrier.is_some() {
            return (vec![], None);
        }
        let mut upstream_epochs_to_notify = vec![];
        while let Some(mut epoch_state) = self.pending_barriers_to_complete.pop_back() {
            let epoch = epoch_state.epoch;
            let is_first = self.initial_epoch.expect("should have set") == epoch;
            if is_first {
                assert!(epoch_state.is_checkpoint);
            } else if !epoch_state.is_checkpoint {
                if let Some(upstream_epoch) = epoch_state.upstream_epoch_to_notify {
                    upstream_epochs_to_notify.push(upstream_epoch);
                }
                continue;
            }

            let resps = take(&mut epoch_state.resps);
            self.completing_barrier = Some(epoch_state);
            return (upstream_epochs_to_notify, Some((epoch, resps, is_first)));
        }
        (upstream_epochs_to_notify, None)
    }

    pub(super) fn ack_completed(&mut self, completed_epoch: u64) -> Option<u64> {
        let epoch_state = self.completing_barrier.take().expect("should exist");
        assert_eq!(epoch_state.epoch, completed_epoch);
        epoch_state.upstream_epoch_to_notify
    }

    fn add_collected(&mut self, epoch_state: CreatingStreamingJobEpochState) {
        assert!(epoch_state.node_to_collect.is_empty());
        if let Some(prev_epoch_state) = self.pending_barriers_to_complete.front() {
            assert!(prev_epoch_state.epoch < epoch_state.epoch);
        }
        if let Some(max_collected_epoch) = self.max_collected_epoch {
            assert!(epoch_state.epoch > max_collected_epoch);
        }
        self.max_collected_epoch = Some(epoch_state.epoch);
        self.pending_barriers_to_complete.push_front(epoch_state);
    }
}
