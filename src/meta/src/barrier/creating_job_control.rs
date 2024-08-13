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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::{debug, info};

use crate::barrier::command::CommandContext;
use crate::barrier::info::InflightGraphInfo;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::{
    BarrierKind, Command, CreateStreamingJobCommandInfo, SnapshotBackfillInfo, TracedEpoch,
};
use crate::manager::WorkerId;
use crate::model::ActorId;
use crate::MetaResult;

#[derive(Debug)]
struct CreatingStreamingJobEpochState {
    node_to_collect: HashSet<WorkerId>,
    resps: Vec<BarrierCompleteResponse>,
}

#[derive(Debug)]
pub(super) enum CreatingStreamingJobStatus {
    ConsumingSnapshot {
        prev_epoch_fake_physical_time: u64,
        pending_commands: Vec<Arc<CommandContext>>,
        version_stats: HummockVersionStats,
        create_mview_tracker: CreateMviewProgressTracker,
        graph_info: InflightGraphInfo,
        /// The `prev_epoch` of pending non checkpoint barriers
        pending_non_checkpoint_barriers: Vec<u64>,
        snapshot_backfill_actors: HashMap<WorkerId, HashSet<ActorId>>,
    },
    ConsumingLogStore {
        graph_info: InflightGraphInfo,
        start_consume_log_store_epoch: u64,
    },
    ConsumingUpstream {
        graph_info: InflightGraphInfo,
        // new epoch at the back
        unattached_epoch: VecDeque<u64>,
        // new epoch at the back. Each item is (creating job epoch, global epoch)
        attached_epoch: VecDeque<(u64, u64)>,
    },
    Finishing {
        // new epoch at the back. Each item is (creating job epoch, global epoch)
        attached_epoch: VecDeque<(u64, u64)>,
    },
}

#[derive(Debug)]
pub(super) struct CreatingStreamingJobControl {
    pub(super) info: CreateStreamingJobCommandInfo,
    pub(super) snapshot_backfill_info: SnapshotBackfillInfo,
    // key is prev_epoch of barrier
    inflight_barrier_queue: BTreeMap<u64, CreatingStreamingJobEpochState>,
    max_collected_epoch: Option<u64>,
    pub(super) collected_barrier: Vec<(u64, Vec<BarrierCompleteResponse>)>,
    backfill_epoch: Epoch,
    pub(super) status: CreatingStreamingJobStatus,
}

impl CreatingStreamingJobControl {
    pub(super) fn new(
        info: CreateStreamingJobCommandInfo,
        snapshot_backfill_info: SnapshotBackfillInfo,
        backfill_epoch: Epoch,
        version_stat: &HummockVersionStats,
    ) -> Self {
        info!(
            table_id = info.table_fragments.table_id().table_id,
            definition = info.definition,
            "new creating job"
        );
        let mut create_mview_tracker = CreateMviewProgressTracker::default();
        create_mview_tracker.update_tracking_jobs(Some((&info, None)), [], version_stat);
        let fragment_info: HashMap<_, _> = info.new_fragment_info().collect();
        let snapshot_backfill_actors_set = info.table_fragments.snapshot_backfill_actor_ids();
        let mut snapshot_backfill_actors: HashMap<_, HashSet<_>> = HashMap::new();
        for fragment in fragment_info.values() {
            for (actor_id, worker_node) in &fragment.actors {
                if snapshot_backfill_actors_set.contains(actor_id) {
                    snapshot_backfill_actors
                        .entry(*worker_node)
                        .or_default()
                        .insert(*actor_id);
                }
            }
        }

        Self {
            info,
            snapshot_backfill_info,
            inflight_barrier_queue: Default::default(),
            max_collected_epoch: None,
            collected_barrier: vec![],
            backfill_epoch,
            status: CreatingStreamingJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time: 0,
                pending_commands: vec![],
                version_stats: version_stat.clone(),
                create_mview_tracker,
                graph_info: InflightGraphInfo::new(fragment_info),
                pending_non_checkpoint_barriers: vec![],
                snapshot_backfill_actors,
            },
        }
    }

    pub(super) fn is_wait_on_worker(&self, worker_id: WorkerId) -> bool {
        self.inflight_barrier_queue
            .values()
            .any(|epoch_state| epoch_state.node_to_collect.contains(&worker_id))
            || {
                match &self.status {
                    CreatingStreamingJobStatus::ConsumingSnapshot { graph_info, .. }
                    | CreatingStreamingJobStatus::ConsumingLogStore { graph_info, .. }
                    | CreatingStreamingJobStatus::ConsumingUpstream { graph_info, .. } => {
                        graph_info.contains_worker(worker_id)
                    }
                    CreatingStreamingJobStatus::Finishing { .. } => false,
                }
            }
    }

    pub(super) fn on_new_worker_node_map(&self, node_map: &HashMap<WorkerId, WorkerNode>) {
        match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { graph_info, .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { graph_info, .. }
            | CreatingStreamingJobStatus::ConsumingUpstream { graph_info, .. } => {
                graph_info.on_new_worker_node_map(node_map)
            }
            CreatingStreamingJobStatus::Finishing { .. } => {}
        }
    }

    fn latest_epoch(&self) -> Option<u64> {
        self.inflight_barrier_queue
            .last_key_value()
            .map(|(epoch, _)| *epoch)
            .or(self.max_collected_epoch)
    }

    pub(super) fn gen_ddl_progress(&self) -> DdlProgress {
        let progress = match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                ..
            } => {
                if create_mview_tracker.has_pending_finished_jobs() {
                    "Snapshot finished".to_string()
                } else {
                    let progress = create_mview_tracker
                        .gen_ddl_progress()
                        .remove(&self.info.table_fragments.table_id().table_id)
                        .expect("should exist");
                    format!("Snapshot [{}]", progress.progress)
                }
            }
            CreatingStreamingJobStatus::ConsumingLogStore {
                start_consume_log_store_epoch,
                ..
            } => {
                let max_collected_epoch = self
                    .max_collected_epoch
                    .expect("should have collected some epoch when entering ConsumingLogStore");
                let lag = Duration::from_millis(
                    Epoch(*start_consume_log_store_epoch)
                        .physical_time()
                        .saturating_sub(Epoch(max_collected_epoch).physical_time()),
                );
                format!(
                    "LogStore [remain lag: {:?}, epoch cnt: {}]",
                    lag,
                    self.inflight_barrier_queue.len()
                )
            }
            CreatingStreamingJobStatus::ConsumingUpstream {
                unattached_epoch, ..
            } => {
                format!(
                    "Upstream [unattached: {}, epoch cnt: {}]",
                    self.inflight_barrier_queue.len(),
                    unattached_epoch.len()
                )
            }
            CreatingStreamingJobStatus::Finishing { .. } => {
                format!(
                    "Finished [epoch count: {}]",
                    self.inflight_barrier_queue.len()
                )
            }
        };
        DdlProgress {
            id: self.info.table_fragments.table_id().table_id as u64,
            statement: self.info.definition.clone(),
            progress,
        }
    }

    pub(super) fn backfill_progress(&self) -> Option<u64> {
        match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. } => Some(self.backfill_epoch.0),
            CreatingStreamingJobStatus::ConsumingLogStore { .. } => {
                if let Some(max_collected_epoch) = self.max_collected_epoch
                    && max_collected_epoch > self.backfill_epoch.0
                {
                    Some(max_collected_epoch)
                } else {
                    Some(self.backfill_epoch.0)
                }
            }
            CreatingStreamingJobStatus::ConsumingUpstream { .. }
            | CreatingStreamingJobStatus::Finishing { .. } => {
                if self.inflight_barrier_queue.is_empty() {
                    None
                } else if let Some(max_collected_epoch) = self.max_collected_epoch
                    && max_collected_epoch > self.backfill_epoch.0
                {
                    Some(max_collected_epoch)
                } else {
                    Some(self.backfill_epoch.0)
                }
            }
        }
    }

    pub(super) fn may_inject_fake_barrier(
        &mut self,
        control_stream_manager: &mut ControlStreamManager,
        is_checkpoint: bool,
        global_prev_epoch: u64,
    ) -> MetaResult<()> {
        if let CreatingStreamingJobStatus::ConsumingSnapshot {
            prev_epoch_fake_physical_time,
            pending_commands,
            create_mview_tracker,
            graph_info,
            pending_non_checkpoint_barriers,
            ..
        } = &mut self.status
        {
            let table_id = Some(self.info.table_fragments.table_id());
            if create_mview_tracker.has_pending_finished_jobs() {
                pending_non_checkpoint_barriers.push(self.backfill_epoch.0);

                let prev_epoch = Epoch::from_physical_time(*prev_epoch_fake_physical_time);
                let node_to_collect = control_stream_manager.inject_barrier(
                    table_id,
                    None,
                    (
                        &TracedEpoch::new(self.backfill_epoch),
                        &TracedEpoch::new(prev_epoch),
                    ),
                    &BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers)),
                    graph_info,
                    Some(graph_info),
                    HashMap::new(),
                )?;
                let graph_info = take(graph_info);
                let pending_commands = take(pending_commands);
                self.enqueue_epoch(prev_epoch.0, node_to_collect);
                // finish consuming snapshot
                for command in pending_commands {
                    let node_to_collect = control_stream_manager.inject_barrier(
                        table_id,
                        command.to_mutation(),
                        (&command.curr_epoch, &command.prev_epoch),
                        &command.kind,
                        &graph_info,
                        Some(&graph_info),
                        HashMap::new(),
                    )?;
                    self.enqueue_epoch(command.prev_epoch.value().0, node_to_collect);
                }
                self.status = CreatingStreamingJobStatus::ConsumingLogStore {
                    graph_info,
                    start_consume_log_store_epoch: global_prev_epoch,
                };
            } else {
                let prev_epoch =
                    TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
                *prev_epoch_fake_physical_time += 1;
                let curr_epoch =
                    TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
                pending_non_checkpoint_barriers.push(prev_epoch.value().0);
                let kind = if is_checkpoint {
                    BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers))
                } else {
                    BarrierKind::Barrier
                };
                let node_to_collect = control_stream_manager.inject_barrier(
                    table_id,
                    None,
                    (&curr_epoch, &prev_epoch),
                    &kind,
                    graph_info,
                    Some(graph_info),
                    HashMap::new(),
                )?;
                self.enqueue_epoch(prev_epoch.value().0, node_to_collect);
            }
        }
        Ok(())
    }

    pub(super) fn on_new_command(
        &mut self,
        control_stream_manager: &mut ControlStreamManager,
        command_ctx: &Arc<CommandContext>,
    ) -> MetaResult<Option<Option<InflightGraphInfo>>> {
        let table_id = self.info.table_fragments.table_id();
        let start_consume_upstream = if let Command::MergeSnapshotBackfillStreamingJobs(
            jobs_to_merge,
        ) = &command_ctx.command
        {
            jobs_to_merge.contains_key(&table_id)
        } else {
            false
        };
        let graph_to_finish = match &mut self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                pending_commands, ..
            } => {
                assert!(
                    !start_consume_upstream,
                    "should not start consuming upstream for a job that are consuming snapshot"
                );
                pending_commands.push(command_ctx.clone());
                None
            }
            CreatingStreamingJobStatus::ConsumingLogStore { graph_info, .. } => {
                let node_to_collect = control_stream_manager.inject_barrier(
                    Some(table_id),
                    if start_consume_upstream {
                        // erase the mutation on upstream except the last command
                        command_ctx.to_mutation()
                    } else {
                        None
                    },
                    (&command_ctx.curr_epoch, &command_ctx.prev_epoch),
                    &command_ctx.kind,
                    graph_info,
                    Some(graph_info),
                    HashMap::new(),
                )?;
                if start_consume_upstream {
                    let graph_info = take(graph_info);
                    let unattached_epoch = self
                        .inflight_barrier_queue
                        .keys()
                        .cloned()
                        .chain([command_ctx.prev_epoch.value().0])
                        .skip_while(|epoch| *epoch < self.backfill_epoch.0)
                        .collect();
                    self.status = CreatingStreamingJobStatus::ConsumingUpstream {
                        graph_info,
                        unattached_epoch,
                        attached_epoch: VecDeque::new(),
                    };
                }
                self.enqueue_epoch(command_ctx.prev_epoch.value().0, node_to_collect);
                None
            }
            CreatingStreamingJobStatus::ConsumingUpstream {
                graph_info,
                unattached_epoch,
                attached_epoch,
            } => {
                assert!(
                    !start_consume_upstream,
                    "should not start consuming upstream for a job again"
                );

                let prev_epoch = command_ctx.prev_epoch.value().0;
                unattached_epoch.push_back(prev_epoch);

                let mut epoch_to_attach = *unattached_epoch.front().expect("non-empty");

                let mut remain_count = 5;
                while remain_count > 0
                    && let Some(epoch) = unattached_epoch.pop_front()
                {
                    remain_count -= 1;
                    epoch_to_attach = epoch;
                }
                attached_epoch.push_back((epoch_to_attach, prev_epoch));

                debug!(
                    epoch_to_attach,
                    prev_epoch,
                    table_id = ?self.info.table_fragments.table_id(),
                    "attach epoch"
                );

                let should_finish = command_ctx.kind.is_checkpoint() && unattached_epoch.is_empty();
                let node_to_collect = control_stream_manager.inject_barrier(
                    Some(table_id),
                    command_ctx.to_mutation(),
                    (&command_ctx.curr_epoch, &command_ctx.prev_epoch),
                    &command_ctx.kind,
                    graph_info,
                    if should_finish {
                        None
                    } else {
                        Some(graph_info)
                    },
                    HashMap::new(),
                )?;
                let graph_info = if should_finish {
                    debug!(prev_epoch = command_ctx.prev_epoch.value().0, table_id = ?self.info.table_fragments.table_id(), "mark as finishing");
                    assert!(unattached_epoch.is_empty());
                    let graph_info = take(graph_info);
                    self.status = CreatingStreamingJobStatus::Finishing {
                        attached_epoch: take(attached_epoch),
                    };
                    Some(Some(graph_info))
                } else {
                    Some(None)
                };
                self.enqueue_epoch(command_ctx.prev_epoch.value().0, node_to_collect);
                graph_info
            }
            CreatingStreamingJobStatus::Finishing { .. } => {
                assert!(
                    !start_consume_upstream,
                    "should not start consuming upstream for a job again"
                );
                None
            }
        };
        Ok(graph_to_finish)
    }

    pub(super) fn enqueue_epoch(&mut self, epoch: u64, node_to_collect: HashSet<WorkerId>) {
        debug!(
            epoch,
            ?node_to_collect,
            table_id = self.info.table_fragments.table_id().table_id,
            "creating job enqueue epoch"
        );
        if let Some(latest_epoch) = self.latest_epoch() {
            assert!(epoch > latest_epoch, "{} {}", epoch, latest_epoch);
        }
        if node_to_collect.is_empty() {
            self.collected_barrier.push((epoch, vec![]));
            if let Some(max_collected_epoch) = self.max_collected_epoch {
                assert!(epoch > max_collected_epoch);
            }
            self.max_collected_epoch = Some(epoch);
        } else {
            self.inflight_barrier_queue.insert(
                epoch,
                CreatingStreamingJobEpochState {
                    node_to_collect,
                    resps: vec![],
                },
            );
        }
    }

    pub(super) fn all_collected(&self) -> bool {
        self.inflight_barrier_queue.is_empty()
    }

    pub(super) fn collect(
        &mut self,
        epoch: u64,
        worker_id: WorkerId,
        resp: BarrierCompleteResponse,
    ) -> Option<(Vec<u64>, bool)> {
        debug!(
            epoch,
            worker_id,
            table_id = self.info.table_fragments.table_id().table_id,
            "collect barrier from worker"
        );

        if let CreatingStreamingJobStatus::ConsumingSnapshot {
            create_mview_tracker,
            version_stats,
            ..
        } = &mut self.status
        {
            create_mview_tracker.update_tracking_jobs(
                None,
                resp.create_mview_progress.iter(),
                version_stats,
            );
        }

        let state = self
            .inflight_barrier_queue
            .get_mut(&epoch)
            .expect("should exist");
        assert!(state.node_to_collect.remove(&worker_id));
        state.resps.push(resp);
        while let Some((_, state)) = self.inflight_barrier_queue.first_key_value() {
            if state.node_to_collect.is_empty() {
                let (epoch, state) = self.inflight_barrier_queue.pop_first().expect("non-empty");
                self.collected_barrier.push((epoch, state.resps));
                if let Some(max_collected_epoch) = self.max_collected_epoch {
                    assert!(epoch > max_collected_epoch);
                }
                self.max_collected_epoch = Some(epoch);
            } else {
                break;
            }
        }
        debug!(
            epoch,
            worker_id,
            collected = ?self.collected_barrier.iter().map(|(epoch, _)| *epoch).collect_vec(),
            inflight = ?self.inflight_barrier_queue.keys().collect_vec(),
            "collect"
        );
        if let Some(max_collected_epoch) = self.max_collected_epoch {
            match &mut self.status {
                CreatingStreamingJobStatus::ConsumingSnapshot { .. }
                | CreatingStreamingJobStatus::ConsumingLogStore { .. } => None,
                CreatingStreamingJobStatus::ConsumingUpstream { attached_epoch, .. }
                | CreatingStreamingJobStatus::Finishing { attached_epoch } => {
                    let mut epochs_to_notify = Vec::new();
                    while let Some((job_epoch, _)) = attached_epoch.front()
                        && max_collected_epoch >= *job_epoch
                    {
                        let (_, global_epoch) = attached_epoch.pop_front().expect("non-empty");
                        epochs_to_notify.push(global_epoch);
                    }
                    if epochs_to_notify.is_empty() {
                        None
                    } else {
                        let is_finish = attached_epoch.is_empty()
                            && matches!(&self.status, CreatingStreamingJobStatus::Finishing { .. });
                        debug!(?epochs_to_notify, is_finish, "notify collect epoch");
                        Some((epochs_to_notify, is_finish))
                    }
                }
            }
        } else {
            None
        }
    }

    pub(super) fn should_merge_to_upstream(&self) -> Option<InflightGraphInfo> {
        if let CreatingStreamingJobStatus::ConsumingLogStore {
            graph_info,
            start_consume_log_store_epoch,
        } = &self.status
        {
            let max_collected_epoch = self
                .max_collected_epoch
                .expect("should have collected some epoch when entering `ConsumingLogStore`");
            if max_collected_epoch >= *start_consume_log_store_epoch {
                Some(graph_info.clone())
            } else {
                let lag = Duration::from_millis(
                    Epoch(*start_consume_log_store_epoch).physical_time()
                        - Epoch(max_collected_epoch).physical_time(),
                );
                debug!(
                    ?lag,
                    max_collected_epoch, start_consume_log_store_epoch, "wait consuming log store"
                );
                None
            }
        } else {
            None
        }
    }
}
