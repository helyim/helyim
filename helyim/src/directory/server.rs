use std::{net::SocketAddr, pin::Pin, result::Result as StdResult, sync::Arc, time::Duration};

use axum::{extract::DefaultBodyLimit, middleware::from_fn_with_state, routing::get, Router};
use dashmap::DashMap;
use faststr::FastStr;
use futures::{
    channel::mpsc::{unbounded, UnboundedSender},
    Stream, StreamExt,
};
use helyim_proto::directory::{
    helyim_server::{Helyim, HelyimServer},
    lookup_ec_volume_response::EcShardIdLocation,
    lookup_volume_response::VolumeIdLocation,
    HeartbeatRequest, HeartbeatResponse, KeepConnectedRequest, Location, LookupEcVolumeRequest,
    LookupEcVolumeResponse, LookupVolumeRequest, LookupVolumeResponse, VolumeLocation,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Server as TonicServer, Request, Response, Status, Streaming};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::{error, info};

use crate::{
    client::MasterClient,
    directory::api::{
        assign_handler, cluster_status_handler, dir_status_handler, lookup_handler, DirectoryState,
    },
    errors::Result,
    raft::{create_raft_router, RaftServer},
    sequence::Sequencer,
    storage::VolumeError,
    topology::{
        node::Node, topology_vacuum_loop, volume_grow::VolumeGrowth, DataNodeRef, Topology,
        TopologyError, TopologyRef,
    },
    util::{
        args::MasterOptions,
        get_or_default,
        grpc::grpc_port,
        http::{default_handler, extractor::require_leader},
        parser::parse_vid_fid,
        sys::exit,
    },
};

pub struct DirectoryServer {
    pub options: Arc<MasterOptions>,
    pub garbage_threshold: f64,
    pub topology: TopologyRef,
    pub volume_grow: VolumeGrowth,
    pub master_client: Arc<MasterClient>,

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

        tokio::spawn(topology_vacuum_loop(
            topology.clone(),
            garbage_threshold,
            volume_size_limit_mb * (1 << 20),
            shutdown_rx.clone(),
        ));

        let master_client = MasterClient::new("master", master_opts.raft.peers.clone());
        let master = DirectoryServer {
            options: master_opts,
            garbage_threshold,
            volume_grow: VolumeGrowth,
            topology: topology.clone(),
            master_client: Arc::new(master_client),
            shutdown,
        };

        let addr = format!("{}:{}", master.options.ip, grpc_port(master.options.port)).parse()?;
        tokio::spawn(async move {
            info!("directory grpc server starting up. binding addr: {addr}");
            if let Err(err) = TonicServer::builder()
                .add_service(HelyimServer::new(DirectoryGrpcServer {
                    volume_size_limit_mb,
                    topology,
                    client_chans: Arc::new(DashMap::new()),
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
        let raft_node_addr = format!("{}:{}", self.options.ip, self.options.port);
        // start raft node and control cluster with `raft_client`
        let raft_server = RaftServer::start_node(&raft_node_addr).await?;
        raft_server.set_topology(self.topology.clone()).await;
        self.topology.set_raft_server(raft_server.clone()).await;

        // http server
        let state = DirectoryState {
            topology: self.topology.clone(),
            volume_grow: self.volume_grow,
            options: self.options.clone(),
        };
        let addr = format!("{}:{}", self.options.ip, self.options.port).parse()?;
        let shutdown_rx = self.shutdown.new_receiver();
        let raft_router = create_raft_router(raft_server.clone());

        tokio::spawn(start_directory_server(
            state,
            addr,
            shutdown_rx,
            raft_router,
        ));

        raft_server
            .start_node_with_peers(&raft_node_addr, &self.options.raft.peers)
            .await?;

        let master_client = self.master_client.clone();
        tokio::spawn(async move {
            master_client.keep_connected_to_master().await;
        });
        Ok(())
    }
}

async fn start_directory_server(
    state: DirectoryState,
    addr: SocketAddr,
    mut shutdown: async_broadcast::Receiver<()>,
    raft_router: Router,
) {
    let http_router = Router::new()
        .route(
            "/dir/assign",
            get(assign_handler)
                .post(assign_handler)
                .layer(from_fn_with_state(state.clone(), require_leader)),
        )
        .route(
            "/dir/lookup",
            get(lookup_handler)
                .post(lookup_handler)
                .layer(from_fn_with_state(state.clone(), require_leader)),
        )
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
        .with_state(state);

    let app = http_router.merge(Router::new().nest("/raft", raft_router));

    info!("directory api server is starting up. binding addr: {addr}");
    match TcpListener::bind(addr).await {
        Ok(listener) => {
            // will blocking current thread
            if let Err(err) = axum::serve(listener, app.into_make_service())
                .with_graceful_shutdown(async move {
                    let _ = shutdown.recv().await;
                    info!("directory api server shutting down gracefully.");
                })
                .await
            {
                error!("starting directory api server failed, error: {err}");
                exit();
            }
        }
        Err(err) => error!("binding directory api address {addr} failed, error: {err}"),
    }
}

#[derive(Clone)]
struct DirectoryGrpcServer {
    pub volume_size_limit_mb: u64,
    pub topology: TopologyRef,
    pub client_chans: Arc<DashMap<FastStr, UnboundedSender<VolumeLocation>>>,
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

        let client_chans = self.client_chans.clone();
        tokio::spawn(async move {
            let mut data_node_opt = None;
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(heartbeat) => {
                        if !topology.is_leader().await {
                            match heartbeat_response(volume_size_limit, &topology).await {
                                Ok(response) => {
                                    let _ = tx.send(Ok(response));
                                    continue;
                                }
                                Err(err) => {
                                    error!("create heartbeat response error: {err}");
                                    break;
                                }
                            }
                        }
                        topology.set_max_sequence(heartbeat.max_file_key);

                        match data_node_opt.as_ref() {
                            Some(data_node) => {
                                if let Err(err) = update_volume_layout(
                                    &heartbeat,
                                    &topology,
                                    &client_chans,
                                    data_node,
                                )
                                .await
                                {
                                    error!("update volume layout error: {err}");
                                }
                            }
                            None => {
                                if let Ok(data_node) =
                                    update_topology(&heartbeat, &topology, addr).await
                                {
                                    info!(
                                        "register connected volume server: {}:{}",
                                        data_node.ip, data_node.port
                                    );

                                    data_node_opt = Some(data_node);
                                }
                            }
                        }

                        match heartbeat_response(volume_size_limit, &topology).await {
                            Ok(response) => {
                                let _ = tx.send(Ok(response));
                                continue;
                            }
                            Err(err) => {
                                error!("create heartbeat response error: {err}");
                                break;
                            }
                        }
                    }
                    Err(err) => {
                        if let Err(e) = tx.send(Err(err)) {
                            error!("heartbeat response dropped: {e}");
                            break;
                        }
                    }
                }
            }
            remove_data_node(&topology, &client_chans, data_node_opt).await;
        });

        let out_stream = UnboundedReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream) as Self::HeartbeatStream))
    }

    // keep_connected keep a stream gRPC call to the master. Used by clients to know the master is
    // up. And clients gets the up-to-date list of volume locations
    type KeepConnectedStream =
        Pin<Box<dyn Stream<Item = StdResult<VolumeLocation, Status>> + Send>>;
    async fn keep_connected(
        &self,
        request: Request<Streaming<KeepConnectedRequest>>,
    ) -> StdResult<Response<Self::KeepConnectedStream>, Status> {
        let topology = self.topology.clone();
        let addr = request.remote_addr().unwrap();
        let clients = self.client_chans.clone();

        let mut in_stream = request.into_inner();
        let (location_tx, location_rx) = tokio::sync::mpsc::unbounded_channel();

        let (message_tx, mut message_rx) = unbounded();
        let (stop_tx, mut stop_rx) = unbounded();

        let client_name = match in_stream.next().await {
            Some(Ok(request)) => {
                if !topology.is_leader().await {
                    topology.inform_new_leader(&location_tx).await;
                    return Err(Status::internal("current node is not raft leader"));
                }
                let client_name = FastStr::new(format!("{}.{addr}", request.name));
                info!("add client: {client_name}");

                clients.insert(client_name.clone(), message_tx);

                for location in topology.to_volume_locations() {
                    if let Err(err) = location_tx.send(Ok(location)) {
                        error!("send keep connected response error: {err}");
                        return Err(Status::internal(err.to_string()));
                    }
                }
                client_name
            }
            Some(Err(err)) => {
                error!("keep connect error: {err}");
                return Err(err);
            }
            None => {
                return Err(Status::internal("keep connect error"));
            }
        };

        let tmp_client_name = client_name.clone();
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                if result.is_err() {
                    let _ = stop_tx.unbounded_send(());
                    break;
                }
            }
            clients.remove(&tmp_client_name);
            info!("remove client: {tmp_client_name}");
        });

        let tmp_client_name = client_name.clone();
        let clients = self.client_chans.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if !topology.is_leader().await {
                            topology.inform_new_leader(&location_tx).await;
                            break;
                        }
                    }
                    _ = stop_rx.next() => {
                        break;
                    }
                    Some(location) = message_rx.next() => {
                        if let Err(err) = location_tx.send(Ok(location)) {
                            error!("send volume location error: {err}, client: {tmp_client_name}");
                            break;
                        }
                    }
                }
            }
            clients.remove(&tmp_client_name);
        });

        let out_stream = UnboundedReceiverStream::new(location_rx);
        Ok(Response::new(
            Box::pin(out_stream) as Self::KeepConnectedStream
        ))
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

// handle heartbeat
async fn update_topology(
    heartbeat: &HeartbeatRequest,
    topology: &TopologyRef,
    addr: SocketAddr,
) -> StdResult<DataNodeRef, VolumeError> {
    topology.set_max_sequence(heartbeat.max_file_key);
    let mut ip = heartbeat.ip.clone();
    if heartbeat.ip.is_empty() {
        ip = addr.ip().to_string();
    }

    let data_center = get_or_default(&heartbeat.data_center);
    let rack = get_or_default(&heartbeat.rack);

    let data_center = topology.get_or_create_data_center(&data_center).await?;
    data_center.set_parent(Some(topology.clone())).await;

    let rack = data_center.get_or_create_rack(&rack).await?;
    rack.set_parent(Some(data_center)).await;

    let node_addr = format!("{}:{}", ip, heartbeat.port);
    let data_node = rack
        .get_or_create_data_node(
            FastStr::new(node_addr),
            FastStr::new(ip),
            heartbeat.port as u16,
            FastStr::new(&heartbeat.public_url),
            heartbeat.max_volume_count as i64,
        )
        .await;
    data_node.set_parent(Some(rack.clone())).await;
    Ok(data_node)
}

async fn update_volume_layout(
    heartbeat: &HeartbeatRequest,
    topology: &TopologyRef,
    client_chans: &Arc<DashMap<FastStr, UnboundedSender<VolumeLocation>>>,
    data_node: &DataNodeRef,
) -> StdResult<(), VolumeError> {
    let mut volume_location = VolumeLocation::new();
    volume_location.url = data_node.url();
    volume_location.public_url = data_node.public_url.to_string();

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

        if !volume_location.new_vids.is_empty() || !volume_location.deleted_vids.is_empty() {
            info!(
                "incremental sync data node: {} registration, add volumes: {:?}, remove volumes: \
                 {:?}",
                data_node.url(),
                volume_location.new_vids,
                volume_location.deleted_vids
            );
        }
    }

    if !heartbeat.volumes.is_empty() || heartbeat.has_no_volumes {
        if !heartbeat.ip.is_empty() {
            topology
                .register_data_node(&heartbeat.data_center, &heartbeat.rack, data_node)
                .await?;
        }

        let (new_volumes, deleted_volumes) = topology
            .sync_data_node_registration(&heartbeat.volumes, data_node)
            .await;
        for volume in new_volumes {
            volume_location.new_vids.push(volume.id);
        }
        for volume in deleted_volumes {
            volume_location.deleted_vids.push(volume.id);
        }

        if !volume_location.new_vids.is_empty() || !volume_location.deleted_vids.is_empty() {
            info!(
                "sync data node: {} registration, add volumes: {:?}, remove volumes: {:?}",
                data_node.url(),
                volume_location.new_vids,
                volume_location.deleted_vids
            );
        }
    }

    if !heartbeat.new_ec_shards.is_empty() || !heartbeat.deleted_ec_shards.is_empty() {
        // update master interval volume layouts
        topology
            .incremental_sync_data_node_ec_shards(
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

        if !volume_location.new_ec_vids.is_empty() || !volume_location.deleted_ec_vids.is_empty() {
            info!(
                "incremental sync data node: {} ec shards registration, add ec volumes: {:?}, \
                 remove ec volumes: {:?}",
                data_node.url(),
                volume_location.new_ec_vids,
                volume_location.deleted_ec_vids
            );
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

        if !volume_location.new_ec_vids.is_empty() || !volume_location.deleted_ec_vids.is_empty() {
            info!(
                "sync data node: {} ec_shards, add ec volumes: {:?}, remove ec volumes: {:?}",
                data_node.url(),
                volume_location.new_ec_vids,
                volume_location.deleted_ec_vids
            );
        }
    }

    if !volume_location.new_vids.is_empty()
        || !volume_location.deleted_vids.is_empty()
        || !volume_location.new_ec_vids.is_empty()
        || !volume_location.deleted_ec_vids.is_empty()
    {
        for client in client_chans.iter() {
            let _ = client.value().unbounded_send(volume_location.clone());
        }
    }

    Ok(())
}

async fn remove_data_node(
    topology: &TopologyRef,
    client_chans: &Arc<DashMap<FastStr, UnboundedSender<VolumeLocation>>>,
    data_node_opt: Option<DataNodeRef>,
) {
    if let Some(data_node) = data_node_opt {
        info!(
            "unregister disconnected volume server: {}:{}",
            data_node.ip, data_node.port
        );

        topology.unregister_data_node(&data_node).await;

        let mut volume_location = VolumeLocation::new();
        volume_location.url = data_node.url();
        volume_location.public_url = data_node.public_url.to_string();

        for volume in data_node.volumes.iter() {
            volume_location.deleted_vids.push(volume.id);
        }
        for shard in data_node.ec_shards.iter() {
            volume_location.deleted_vids.push(shard.volume_id);
        }

        if !volume_location.deleted_vids.is_empty() {
            for client in client_chans.iter() {
                let _ = client.value().unbounded_send(volume_location.clone());
            }
        }
    }
}

async fn heartbeat_response(
    volume_size_limit: u64,
    topology: &TopologyRef,
) -> StdResult<HeartbeatResponse, TopologyError> {
    let leader = topology.current_leader().await?;
    Ok(HeartbeatResponse {
        volume_size_limit,
        leader: leader.to_string(),
        ..Default::default()
    })
}
