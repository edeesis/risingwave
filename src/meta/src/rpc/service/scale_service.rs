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

use itertools::Itertools;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::reschedule_request::Reschedule;
use risingwave_pb::meta::scale_service_server::ScaleService;
use risingwave_pb::meta::{
    ClearWorkerNodesRequest, ClearWorkerNodesResponse, GetClusterInfoRequest,
    GetClusterInfoResponse, GetScheduleRequest, GetScheduleResponse, PauseRequest, PauseResponse,
    RescheduleRequest, RescheduleResponse, ResumeRequest, ResumeResponse,
};
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use tonic::{Request, Response, Status};

use crate::barrier::{BarrierScheduler, Command};
use crate::manager::{CatalogManagerRef, ClusterManagerRef, FragmentManagerRef};
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::stream::{GlobalStreamManagerRef, ParallelUnitReschedule, SourceManagerRef};

pub struct ScaleServiceImpl<S: MetaStore> {
    barrier_scheduler: BarrierScheduler<S>,
    fragment_manager: FragmentManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
    source_manager: SourceManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    stream_manager: GlobalStreamManagerRef<S>,
}

impl<S> ScaleServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        barrier_scheduler: BarrierScheduler<S>,
        fragment_manager: FragmentManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        source_manager: SourceManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
        stream_manager: GlobalStreamManagerRef<S>,
    ) -> Self {
        Self {
            barrier_scheduler,
            fragment_manager,
            cluster_manager,
            source_manager,
            catalog_manager,
            stream_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S> ScaleService for ScaleServiceImpl<S>
where
    S: MetaStore,
{
    #[cfg_attr(coverage, no_coverage)]
    async fn pause(&self, _: Request<PauseRequest>) -> Result<Response<PauseResponse>, Status> {
        self.barrier_scheduler.run_command(Command::pause()).await?;
        Ok(Response::new(PauseResponse {}))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn resume(&self, _: Request<ResumeRequest>) -> Result<Response<ResumeResponse>, Status> {
        self.barrier_scheduler
            .run_command(Command::resume())
            .await?;
        Ok(Response::new(ResumeResponse {}))
    }

    // Removes all actors from the listed workers.
    // This is required before shutting down the workers.
    // Not atomic! Will abort on encountering first error, thus partially clearing workers
    #[cfg_attr(coverage, no_coverage)]
    async fn clear_workers(
        &self,
        request: Request<ClearWorkerNodesRequest>,
    ) -> Result<Response<ClearWorkerNodesResponse>, Status> {
        assert!(false);
        // TODO: we also do this in rw cloud. We can move that functionality from cloud to meta
        // see branch arne/scaling/placement-policy meta.go ClearWorkerNode

        let schedule = "";

        Ok(Response::new(ClearWorkerNodesResponse { status: None }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn get_cluster_info(
        &self,
        _: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        let table_fragments = self
            .fragment_manager
            .list_table_fragments()
            .await?
            .iter()
            .map(|tf| tf.to_protobuf())
            .collect();

        let worker_nodes = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, None)
            .await;

        let actor_splits = self
            .source_manager
            .get_actor_splits()
            .await
            .into_iter()
            .map(|(actor_id, splits)| {
                (
                    actor_id,
                    ConnectorSplits {
                        splits: splits.iter().map(ConnectorSplit::from).collect(),
                    },
                )
            })
            .collect();

        let sources = self.catalog_manager.list_sources().await;

        let source_infos = sources.into_iter().map(|s| (s.id, s)).collect();

        Ok(Response::new(GetClusterInfoResponse {
            worker_nodes,
            table_fragments,
            actor_splits,
            source_infos,
        }))
    }

    async fn get_schedule(
        &self,
        _request: Request<GetScheduleRequest>,
    ) -> Result<Response<GetScheduleResponse>, Status> {
        let cluster_info = self
            .get_cluster_info(Request::new(GetClusterInfoRequest {}))
            .await?
            .into_inner();

        // Compile fragments
        let mut fragment_list: Vec<risingwave_pb::common::Fragment> = vec![];
        for table_fragment in cluster_info.get_table_fragments() {
            for (_, fragment) in table_fragment.get_fragments() {
                let mut actor_list: Vec<risingwave_pb::common::Actor> = vec![];
                for actor in fragment.get_actors() {
                    let id = actor.actor_id;
                    let pu_id = table_fragment
                        .get_actor_status()
                        .get(&id)
                        .expect("expected actor status") // TODO: handle gracefully
                        .get_parallel_unit()?
                        .get_id();
                    actor_list.push(risingwave_pb::common::Actor {
                        actor_id: actor.actor_id,
                        parallel_units_id: pu_id,
                    });
                }
                fragment_list.push(risingwave_pb::common::Fragment {
                    id: fragment.get_fragment_id(),
                    actor_list,
                    type_flag: fragment.fragment_type_mask,
                });
            }
        }

        Ok(Response::new(GetScheduleResponse {
            fragment_list: fragment_list,
            worker_list: cluster_info.worker_nodes,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn reschedule(
        &self,
        request: Request<RescheduleRequest>,
    ) -> Result<Response<RescheduleResponse>, Status> {
        let req = request.into_inner();

        self.stream_manager
            .reschedule_actors(
                req.reschedules
                    .into_iter()
                    .map(|(fragment_id, reschedule)| {
                        let Reschedule {
                            added_parallel_units,
                            removed_parallel_units,
                        } = reschedule;

                        (
                            fragment_id,
                            ParallelUnitReschedule {
                                added_parallel_units: added_parallel_units
                                    .into_iter()
                                    .sorted()
                                    .dedup()
                                    .collect(),
                                removed_parallel_units: removed_parallel_units
                                    .into_iter()
                                    .sorted()
                                    .dedup()
                                    .collect(),
                            },
                        )
                    })
                    .collect(),
            )
            .await?;

        Ok(Response::new(RescheduleResponse { success: true }))
    }
}
