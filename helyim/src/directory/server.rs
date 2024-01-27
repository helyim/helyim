use std::{net::SocketAddr, pin::Pin, result::Result as StdResult, sync::Arc, time::Duration};

use axum::{extract::DefaultBodyLimit, routing::get, Router};
use faststr::FastStr;
use futures::{Stream, StreamExt};
use helyim_proto::{
    helyim_server::{Helyim, HelyimServer},
    lookup_ec_volume_response::EcShardIdLocation,
    lookup_volume_response::VolumeIdLocation,
    HeartbeatRequest, HeartbeatResponse, KeepConnectedRequest, Location, LookupEcVolumeRequest,
    LookupEcVolumeResponse, LookupVolumeRequest, LookupVolumeResponse, VolumeLocation,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Server as TonicServer, Request, Response, Status, Streaming};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::{debug, error, info};

use crate::{
    directory::api::{
        assign_handler, cluster_status_handler, dir_status_handler, lookup_handler, DirectoryState,
    },
    errors::Result,
    raft::{create_raft_router, RaftServer},
    rt_spawn,
    sequence::Sequencer,
    storage::VolumeInfo,
    topology::{
        topology_vacuum_loop, volume_grow::VolumeGrowth, DataNodeRef, Topology, TopologyRef,
    },
    util::{
        args::MasterOptions, get_or_default, grpc::grpc_port, http::default_handler,
        parser::parse_vid_fid, sys::exit,
    },
};

pub struct DirectoryServer {
    pub options: Arc<MasterOptions>,
    pub garbage_threshold: f64,
    pub topology: TopologyRef,
    pub volume_grow: VolumeGrowth,

    shutdown: async_broadcast::Sender<()>,
}

impl DirectoryServer {
    pub async fn new(
        options: MasterOptions,
        garbage_threshold: f64,
        sequencer: Sequencer,
    ) -> Result<DirectoryServer> {
        let master_opts = Arc::new(options);

        let (shutdown, mut shutdown_rx) = async_broadcast::broadcast(16);
        let volume_size_limit_mb = master_opts.volume_size_limit_mb;

        let topology = Arc::new(Topology::new(
            sequencer,
            volume_size_limit_mb * 1024 * 1024,
            master_opts.pulse,
        ));

        rt_spawn(topology_vacuum_loop(
            topology.clone(),
            garbage_threshold,
            volume_size_limit_mb * (1 << 20),
            shutdown_rx.clone(),
        ));

        let master = DirectoryServer {
            options: master_opts,
            garbage_threshold,
            volume_grow: VolumeGrowth,
            topology: topology.clone(),
            shutdown,
        };

        let addr = format!("{}:{}", master.options.ip, grpc_port(master.options.port)).parse()?;
        rt_spawn(async move {
            info!("directory grpc server starting up. binding addr: {addr}");
            if let Err(err) = TonicServer::builder()
                .add_service(HelyimServer::new(DirectoryGrpcServer {
                    volume_size_limit_mb,
                    topology,
                }))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.recv().await;
                })
                .await
            {
                error!("directory grpc server starting failed, {err}");
                exit();
            }
        });

        Ok(master)
    }

    pub async fn stop(self) -> Result<()> {
        self.shutdown.broadcast(()).await?;
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let raft_node_id = self.options.raft.node_id;
        let raft_node_addr = format!("{}:{}", self.options.ip, self.options.port);
        // start raft node and control cluster with `raft_client`
        let raft_server = RaftServer::start_node(raft_node_id, &raft_node_addr).await?;
        raft_server.set_topology(&self.topology.clone()).await;
        self.topology.set_raft_server(raft_server.clone()).await;

        // http server
        let ctx = DirectoryState {
            topology: self.topology.clone(),
            volume_grow: self.volume_grow,
            options: self.options.clone(),
        };
        let addr = format!("{}:{}", self.options.ip, self.options.port).parse()?;
        let shutdown_rx = self.shutdown.new_receiver();
        let raft_router = create_raft_router(raft_server.clone());

        rt_spawn(start_directory_server(ctx, addr, shutdown_rx, raft_router));

        raft_server
            .start_node_with_peer(
                raft_node_id,
                &raft_node_addr,
                self.options.raft.peer.as_ref(),
            )
            .await?;

        Ok(())
    }
}

async fn start_directory_server(
    ctx: DirectoryState,
    addr: SocketAddr,
    mut shutdown: async_broadcast::Receiver<()>,
    raft_router: Router,
) {
    let http_router = Router::new()
        .route("/dir/assign", get(assign_handler).post(assign_handler))
        .route("/dir/lookup", get(lookup_handler).post(lookup_handler))
        .route(
            "/dir/status",
            get(dir_status_handler).post(dir_status_handler),
        )
        .route(
            "/cluster/status",
            get(cluster_status_handler).post(cluster_status_handler),
        )
        .fallback(default_handler)
        .layer((
            CompressionLayer::new(),
            DefaultBodyLimit::max(1024 * 1024),
            TimeoutLayer::new(Duration::from_secs(10)),
        ))
        .with_state(ctx);

    let app = http_router.merge(Router::new().nest("/raft", raft_router));

    match hyper::Server::try_bind(&addr) {
        Ok(builder) => {
            let server = builder.serve(app.into_make_service());
            let graceful = server.with_graceful_shutdown(async {
                let _ = shutdown.recv().await;
            });
            info!("directory api server starting up. binding addr: {addr}");
            match graceful.await {
                Ok(()) => info!("directory api server shutting down gracefully."),
                Err(e) => error!("directory api server stop failed, {}", e),
            }
        }
        Err(err) => {
            error!("starting directory api server failed, error: {err}");
            exit();
        }
    }
}

#[derive(Clone)]
struct DirectoryGrpcServer {
    pub volume_size_limit_mb: u64,
    pub topology: TopologyRef,
}

#[tonic::async_trait]
impl Helyim for DirectoryGrpcServer {
    type HeartbeatStream = Pin<Box<dyn Stream<Item = StdResult<HeartbeatResponse, Status>> + Send>>;

    async fn heartbeat(
        &self,
        request: Request<Streaming<HeartbeatRequest>>,
    ) -> StdResult<Response<Self::HeartbeatStream>, Status> {
        let volume_size_limit = self.volume_size_limit_mb * 1024 * 1024;
        let topology = self.topology.clone();
        let addr = request.remote_addr().unwrap();

        let mut in_stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(mut heartbeat) => {
                        if !topology.is_leader().await {
                            let _ =
                                tx.send(Ok(heartbeat_response(volume_size_limit, &topology).await));
                            continue;
                        }
                        debug!("receive {:?}", heartbeat);
                        if let Err(err) = handle_heartbeat(
                            &tx,
                            &mut heartbeat,
                            &topology,
                            volume_size_limit,
                            addr,
                        )
                        .await
                        {
                            error!("handle heartbeat error: {err}");
                        }
                    }
                    Err(err) => {
                        if let Err(e) = tx.send(Err(err)) {
                            error!("heartbeat response dropped: {}", e);
                            return;
                        }
                    }
                }
            }
        });

        let out_stream = UnboundedReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream) as Self::HeartbeatStream))
    }

    type KeepConnectedStream =
        Pin<Box<dyn Stream<Item = StdResult<VolumeLocation, Status>> + Send>>;
    async fn keep_connected(
        &self,
        _request: Request<Streaming<KeepConnectedRequest>>,
    ) -> StdResult<Response<Self::KeepConnectedStream>, Status> {
        todo!()
    }

    async fn lookup_volume(
        &self,
        request: Request<LookupVolumeRequest>,
    ) -> StdResult<Response<LookupVolumeResponse>, Status> {
        let request = request.into_inner();
        if request.volume_or_file_ids.is_empty() {
            return Err(Status::invalid_argument("volumes can't be empty"));
        }

        let mut volume_id_locations = vec![];
        for volume_id in request.volume_or_file_ids {
            let (_, (vid, _fid)) = parse_vid_fid(&volume_id)
                .map_err(|err| Status::invalid_argument(format!("parse volume id error: {err}")))?;
            let vid = vid
                .parse()
                .map_err(|err| Status::invalid_argument(format!("parse volume id error: {err}")))?;

            let mut locations = vec![];
            let mut error = String::default();
            match self.topology.lookup(&request.collection, vid).await {
                Some(nodes) => {
                    for dn in nodes.iter() {
                        let public_url = dn.public_url.to_string();
                        locations.push(Location {
                            url: dn.url(),
                            public_url,
                        });
                    }
                }
                None => {
                    error = format!("volume {vid} is not found");
                }
            }

            volume_id_locations.push(VolumeIdLocation {
                volume_or_file_id: volume_id,
                locations,
                error,
            });
        }

        Ok(Response::new(LookupVolumeResponse {
            volume_id_locations,
        }))
    }

    async fn lookup_ec_volume(
        &self,
        request: Request<LookupEcVolumeRequest>,
    ) -> StdResult<Response<LookupEcVolumeResponse>, Status> {
        if !self.topology.is_leader().await {
            return Err(Status::permission_denied("this node is not raft leader"));
        }
        let request = request.into_inner();
        let ec_locations = match self.topology.lookup_ec_shards(request.volume_id) {
            Some(locations) => locations,
            None => {
                return Err(Status::not_found(format!(
                    "ec volume {} not found",
                    request.volume_id
                )))
            }
        };

        let mut shard_id_locations = Vec::new();
        for (shard_id, shard_locations) in ec_locations.value().locations.iter().enumerate() {
            let mut locations = Vec::new();
            for data_node in shard_locations {
                locations.push(Location {
                    url: data_node.url(),
                    public_url: data_node.public_url.to_string(),
                });
            }
            shard_id_locations.push(EcShardIdLocation {
                locations,
                shard_id: shard_id as u32,
            });
        }

        let response = LookupEcVolumeResponse {
            shard_id_locations,
            volume_id: request.volume_id,
        };
        Ok(Response::new(response))
    }
}

async fn handle_heartbeat(
    tx: &UnboundedSender<StdResult<HeartbeatResponse, Status>>,
    heartbeat: &mut HeartbeatRequest,
    topology: &TopologyRef,
    volume_size_limit: u64,
    addr: SocketAddr,
) -> Result<()> {
    topology.set_max_sequence(heartbeat.max_file_key);
    let mut ip = heartbeat.ip.clone();
    if heartbeat.ip.is_empty() {
        ip = addr.ip().to_string();
    }

    let data_center = get_or_default(&heartbeat.data_center);
    let rack = get_or_default(&heartbeat.rack);

    let data_center = topology.get_or_create_data_center(data_center).await;
    data_center.set_topology(Arc::downgrade(topology)).await;

    let rack = data_center.get_or_create_rack(rack).await;
    rack.set_data_center(Arc::downgrade(&data_center)).await;

    let node_addr = format!("{}:{}", ip, heartbeat.port);
    let data_node = rack
        .get_or_create_data_node(
            FastStr::new(node_addr),
            FastStr::new(ip),
            heartbeat.port as u16,
            FastStr::new(&heartbeat.public_url),
            heartbeat.max_volume_count as i64,
        )
        .await?;
    data_node.set_rack(Arc::downgrade(&rack)).await;

    let mut infos = vec![];
    while let Some(info_msg) = heartbeat.volumes.pop() {
        match VolumeInfo::new(&info_msg) {
            Ok(info) => infos.push(info),
            Err(err) => info!("fail to convert joined volume: {err}"),
        };
    }

    let (new_volumes, deleted_volumes) = data_node.update_volumes(infos).await;

    for v in new_volumes {
        topology.register_volume_layout(&v, &data_node).await;
    }

    for v in deleted_volumes {
        topology.unregister_volume_layout(&v, &data_node).await;
    }

    let _ = tx.send(Ok(heartbeat_response(volume_size_limit, topology).await));

    handle_ec_heartbeat(heartbeat, topology, &data_node).await;
    Ok(())
}

async fn handle_ec_heartbeat(
    heartbeat: &HeartbeatRequest,
    topology: &TopologyRef,
    data_node: &DataNodeRef,
) {
    let mut volume_location = VolumeLocation {
        url: data_node.url(),
        public_url: data_node.public_url.to_string(),
        new_vids: vec![],
        deleted_vids: vec![],
        new_ec_vids: vec![],
        deleted_ec_vids: vec![],
        leader: None,
    };

    if !heartbeat.new_volumes.is_empty() || !heartbeat.deleted_volumes.is_empty() {
        for volume in heartbeat.new_volumes.iter() {
            volume_location.new_vids.push(volume.id);
        }
        for volume in heartbeat.deleted_volumes.iter() {
            volume_location.deleted_vids.push(volume.id);
        }

        topology
            .incremental_sync_data_node_registration(
                &heartbeat.new_volumes,
                &heartbeat.deleted_volumes,
                data_node,
            )
            .await;
    }

    if !heartbeat.volumes.is_empty() || !heartbeat.has_no_volumes {
        let (new_volumes, deleted_volumes) = topology
            .sync_data_node_registration(&heartbeat.volumes, data_node)
            .await;
        for volume in new_volumes {
            volume_location.new_vids.push(volume.id);
        }
        for volume in deleted_volumes {
            volume_location.deleted_vids.push(volume.id);
        }
    }

    if !heartbeat.new_ec_shards.is_empty() || !heartbeat.deleted_ec_shards.is_empty() {
        // update master interval volume layouts
        topology
            .increment_sync_data_node_ec_shards(
                &heartbeat.new_ec_shards,
                &heartbeat.deleted_ec_shards,
                data_node,
            )
            .await;

        for shard in heartbeat.new_ec_shards.iter() {
            volume_location.new_ec_vids.push(shard.id);
        }

        for shard in heartbeat.deleted_ec_shards.iter() {
            if data_node.has_ec_shards(shard.id) {
                continue;
            }
            volume_location.deleted_ec_vids.push(shard.id);
        }
    }

    if !heartbeat.ec_shards.is_empty() || heartbeat.has_no_ec_shards {
        let (new_shards, deleted_shards) = topology
            .sync_data_node_ec_shards(&heartbeat.ec_shards, data_node)
            .await;

        // broadcast the ec vid changes to master clients
        for shard in new_shards {
            volume_location.new_ec_vids.push(shard.volume_id);
        }
        for shard in deleted_shards {
            if data_node.has_ec_shards(shard.volume_id) {
                continue;
            }
            volume_location.deleted_ec_vids.push(shard.volume_id);
        }
    }

    if !volume_location.new_vids.is_empty()
        || !volume_location.deleted_vids.is_empty()
        || !volume_location.new_ec_vids.is_empty()
        || !volume_location.deleted_ec_vids.is_empty()
    {
        // TODO: broadcast to master clients
    }
}

async fn heartbeat_response(volume_size_limit: u64, topology: &TopologyRef) -> HeartbeatResponse {
    let leader = topology.current_leader_address().await.unwrap_or_default();
    HeartbeatResponse {
        volume_size_limit,
        leader: leader.to_string(),
        ..Default::default()
    }
}
