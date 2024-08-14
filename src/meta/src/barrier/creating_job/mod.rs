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

mod barrier_control;

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;

use risingwave_common::util::epoch::Epoch;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::{debug, info};

use crate::barrier::command::CommandContext;
use crate::barrier::creating_job::barrier_control::CreatingStreamingJobBarrierControl;
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
pub(super) enum CreatingStreamingJobStatus {
    ConsumingSnapshot {
        prev_epoch_fake_physical_time: u64,
        pending_commands: Vec<Arc<CommandContext>>,
        version_stats: HummockVersionStats,
        create_mview_tracker: CreateMviewProgressTracker,
        graph_info: InflightGraphInfo,
        backfill_epoch: u64,
        /// The `prev_epoch` of pending non checkpoint barriers
        pending_non_checkpoint_barriers: Vec<u64>,
        snapshot_backfill_actors: HashMap<WorkerId, HashSet<ActorId>>,
    },
    ConsumingLogStore {
        graph_info: InflightGraphInfo,
        start_consume_log_store_epoch: u64,
    },
    ConsumingUpstream {
        start_consume_upstream_epoch: u64,
        graph_info: InflightGraphInfo,
    },
    Finishing {
        start_consume_upstream_epoch: u64,
    },
}

impl CreatingStreamingJobStatus {
    fn active_graph_info(&self) -> Option<&InflightGraphInfo> {
        match self {
            CreatingStreamingJobStatus::ConsumingSnapshot { graph_info, .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { graph_info, .. }
            | CreatingStreamingJobStatus::ConsumingUpstream { graph_info, .. } => Some(graph_info),
            CreatingStreamingJobStatus::Finishing { .. } => {
                // when entering `Finishing`, the graph will have been added to the upstream graph,
                // and therefore the separate graph info is inactive.
                None
            }
        }
    }

    fn update_progress(
        &mut self,
        create_mview_progress: impl IntoIterator<Item = &CreateMviewProgress>,
    ) {
        if let Self::ConsumingSnapshot {
            create_mview_tracker,
            ref version_stats,
            ..
        } = self
        {
            create_mview_tracker.update_tracking_jobs(None, create_mview_progress, version_stats);
        }
    }

    fn may_inject_fake_barrier(
        &mut self,
        upstream_epoch: u64,
        is_checkpoint: bool,
    ) -> Option<Vec<(TracedEpoch, TracedEpoch, BarrierKind)>> {
        if let CreatingStreamingJobStatus::ConsumingSnapshot {
            prev_epoch_fake_physical_time,
            pending_commands,
            create_mview_tracker,
            graph_info,
            pending_non_checkpoint_barriers,
            ref backfill_epoch,
            ..
        } = self
        {
            if create_mview_tracker.has_pending_finished_jobs() {
                pending_non_checkpoint_barriers.push(*backfill_epoch);

                let prev_epoch = Epoch::from_physical_time(*prev_epoch_fake_physical_time);
                let barriers_to_inject = [(
                    TracedEpoch::new(Epoch(*backfill_epoch)),
                    TracedEpoch::new(prev_epoch),
                    BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers)),
                )]
                .into_iter()
                .chain(pending_commands.drain(..).map(|command_ctx| {
                    (
                        command_ctx.curr_epoch.clone(),
                        command_ctx.prev_epoch.clone(),
                        command_ctx.kind.clone(),
                    )
                }))
                .collect();

                let graph_info = take(graph_info);
                *self = CreatingStreamingJobStatus::ConsumingLogStore {
                    graph_info,
                    start_consume_log_store_epoch: upstream_epoch,
                };
                Some(barriers_to_inject)
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
                Some(vec![(curr_epoch, prev_epoch, kind)])
            }
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub(super) struct CreatingStreamingJobControl {
    pub(super) info: CreateStreamingJobCommandInfo,
    pub(super) snapshot_backfill_info: SnapshotBackfillInfo,
    backfill_epoch: u64,

    barrier_control: CreatingStreamingJobBarrierControl,
    status: CreatingStreamingJobStatus,
}

impl CreatingStreamingJobControl {
    pub(super) fn new(
        info: CreateStreamingJobCommandInfo,
        snapshot_backfill_info: SnapshotBackfillInfo,
        backfill_epoch: u64,
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

        let table_id = info.table_fragments.table_id();

        Self {
            info,
            snapshot_backfill_info,
            barrier_control: CreatingStreamingJobBarrierControl::new(table_id),
            backfill_epoch,
            status: CreatingStreamingJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time: 0,
                pending_commands: vec![],
                version_stats: version_stat.clone(),
                create_mview_tracker,
                graph_info: InflightGraphInfo::new(fragment_info),
                backfill_epoch,
                pending_non_checkpoint_barriers: vec![],
                snapshot_backfill_actors,
            },
        }
    }

    pub(super) fn is_wait_on_worker(&self, worker_id: WorkerId) -> bool {
        self.barrier_control.is_wait_on_worker(worker_id)
            || self
                .status
                .active_graph_info()
                .map(|info| info.contains_worker(worker_id))
                .unwrap_or(false)
    }

    pub(super) fn on_new_worker_node_map(&self, node_map: &HashMap<WorkerId, WorkerNode>) {
        if let Some(info) = self.status.active_graph_info() {
            info.on_new_worker_node_map(node_map)
        }
    }

    pub(super) fn status(&self) -> &CreatingStreamingJobStatus {
        &self.status
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
                let max_collected_epoch = max(
                    self.barrier_control.max_collected_epoch().unwrap_or(0),
                    self.backfill_epoch,
                );
                let lag = Duration::from_millis(
                    Epoch(*start_consume_log_store_epoch)
                        .physical_time()
                        .saturating_sub(Epoch(max_collected_epoch).physical_time()),
                );
                format!(
                    "LogStore [remain lag: {:?}, epoch cnt: {}]",
                    lag,
                    self.barrier_control.inflight_barrier_count()
                )
            }
            CreatingStreamingJobStatus::ConsumingUpstream { .. } => {
                format!(
                    "Upstream [unattached: {}, epoch cnt: {}]",
                    self.barrier_control.unsttached_epochs().count(),
                    self.barrier_control.inflight_barrier_count(),
                )
            }
            CreatingStreamingJobStatus::Finishing { .. } => {
                format!(
                    "Finishing [epoch count: {}]",
                    self.barrier_control.inflight_barrier_count()
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
        let stop_consume_log_store_epoch = match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. } => None,
            CreatingStreamingJobStatus::ConsumingUpstream {
                start_consume_upstream_epoch,
                ..
            }
            | CreatingStreamingJobStatus::Finishing {
                start_consume_upstream_epoch,
                ..
            } => Some(*start_consume_upstream_epoch),
        };
        if let Some(max_collected_epoch) = self.barrier_control.max_collected_epoch() {
            if max_collected_epoch < self.backfill_epoch {
                Some(self.backfill_epoch)
            } else if let Some(stop_consume_log_store_epoch) = stop_consume_log_store_epoch
                && max_collected_epoch >= stop_consume_log_store_epoch
            {
                None
            } else {
                Some(max_collected_epoch)
            }
        } else {
            Some(self.backfill_epoch)
        }
    }

    pub(super) fn may_inject_fake_barrier(
        &mut self,
        control_stream_manager: &mut ControlStreamManager,
        upstream_prev_epoch: u64,
        is_checkpoint: bool,
    ) -> MetaResult<()> {
        if let Some(barriers_to_inject) = self
            .status
            .may_inject_fake_barrier(upstream_prev_epoch, is_checkpoint)
        {
            let graph_info = self
                .status
                .active_graph_info()
                .expect("must exist when having barriers to inject");
            let table_id = self.info.table_fragments.table_id();
            for (curr_epoch, prev_epoch, kind) in barriers_to_inject {
                let node_to_collect = control_stream_manager.inject_barrier(
                    Some(table_id),
                    None,
                    (&curr_epoch, &prev_epoch),
                    &kind,
                    graph_info,
                    Some(graph_info),
                    HashMap::new(),
                )?;
                self.barrier_control.enqueue_epoch(
                    prev_epoch.value().0,
                    node_to_collect,
                    kind.is_checkpoint(),
                );
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
                self.barrier_control.enqueue_epoch(
                    command_ctx.prev_epoch.value().0,
                    node_to_collect,
                    command_ctx.kind.is_checkpoint(),
                );
                let prev_epoch = command_ctx.prev_epoch.value().0;
                if start_consume_upstream {
                    let graph_info = take(graph_info);
                    self.status = CreatingStreamingJobStatus::ConsumingUpstream {
                        start_consume_upstream_epoch: prev_epoch,
                        graph_info,
                    };
                }
                None
            }
            CreatingStreamingJobStatus::ConsumingUpstream {
                start_consume_upstream_epoch,
                graph_info,
            } => {
                assert!(
                    !start_consume_upstream,
                    "should not start consuming upstream for a job again"
                );

                let should_finish = command_ctx.kind.is_checkpoint()
                    && self.barrier_control.unsttached_epochs().next().is_none();
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
                let prev_epoch = command_ctx.prev_epoch.value().0;
                self.barrier_control.enqueue_epoch(
                    prev_epoch,
                    node_to_collect,
                    command_ctx.kind.is_checkpoint(),
                );
                let graph_info = if should_finish {
                    debug!(prev_epoch, table_id = ?self.info.table_fragments.table_id(), "mark as finishing");
                    self.barrier_control
                        .attach_upstream_epoch(prev_epoch, prev_epoch);
                    let graph_info = take(graph_info);
                    self.status = CreatingStreamingJobStatus::Finishing {
                        start_consume_upstream_epoch: *start_consume_upstream_epoch,
                    };
                    Some(Some(graph_info))
                } else {
                    let mut unattached_epochs_iter = self.barrier_control.unsttached_epochs();
                    let mut epoch_to_attach = unattached_epochs_iter.next().expect("non-empty").0;
                    let mut remain_count = 5;
                    while remain_count > 0
                        && let Some((epoch, _)) = unattached_epochs_iter.next()
                    {
                        remain_count -= 1;
                        epoch_to_attach = epoch;
                    }
                    drop(unattached_epochs_iter);
                    self.barrier_control
                        .attach_upstream_epoch(epoch_to_attach, prev_epoch);
                    Some(None)
                };

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

    pub(super) fn collect(
        &mut self,
        epoch: u64,
        worker_id: WorkerId,
        resp: BarrierCompleteResponse,
    ) {
        self.status.update_progress(&resp.create_mview_progress);
        self.barrier_control.collect(epoch, worker_id, resp);
    }

    pub(super) fn should_merge_to_upstream(&self) -> Option<InflightGraphInfo> {
        if let (
            CreatingStreamingJobStatus::ConsumingLogStore {
                graph_info,
                start_consume_log_store_epoch,
            },
            Some(max_collected_epoch),
        ) = (&self.status, self.barrier_control.max_collected_epoch())
        {
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

    #[expect(clippy::type_complexity)]
    pub(super) fn start_completing(
        &mut self,
    ) -> (Vec<u64>, Option<(u64, Vec<BarrierCompleteResponse>, bool)>) {
        self.barrier_control.start_completing()
    }

    pub(super) fn ack_completed(&mut self, completed_epoch: u64) -> Option<(u64, bool)> {
        let upstream_epoch_to_notify = self.barrier_control.ack_completed(completed_epoch);
        if let Some(upstream_epoch_to_notify) = upstream_epoch_to_notify {
            Some((upstream_epoch_to_notify, self.is_finished()))
        } else {
            assert!(!self.is_finished());
            None
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.barrier_control.is_empty()
            && matches!(&self.status, CreatingStreamingJobStatus::Finishing { .. })
    }
}
