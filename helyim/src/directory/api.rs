use std::sync::Arc;

use axum::{extract::State, Json};
use faststr::FastStr;

use crate::{
    operation::{
        lookup::{Location, Lookup, LookupRequest},
        AssignRequest, Assignment, ClusterStatus,
    },
    storage::VolumeError,
    topology::{volume_grow::VolumeGrowth, Topology, TopologyRef},
    util::{args::MasterOptions, http::extractor::FormOrJson},
};

#[derive(Clone)]
pub struct DirectoryState {
    pub topology: TopologyRef,
    pub volume_grow: VolumeGrowth,
    pub options: Arc<MasterOptions>,
}

pub async fn assign_handler(
    State(ctx): State<DirectoryState>,
    FormOrJson(request): FormOrJson<AssignRequest>,
) -> Result<Json<Assignment>, VolumeError> {
    let count = match request.count {
        Some(n) if n > 1 => n,
        _ => 1,
    };
    let option = request.volume_grow_option(&ctx.options.default_replication)?;

    if !ctx.topology.has_writable_volume(&option).await {
        if ctx.topology.free_space() <= 0 {
            return Err(VolumeError::NoFreeSpace("no free volumes".to_string()));
        }
        let a = ctx
            .volume_grow
            .grow_by_type(&option, ctx.topology.as_ref())
            .await;
        println!("{:?}", a);
    }
    let (fid, count, node) = ctx.topology.pick_for_write(count, &option).await?;
    let assignment = Assignment {
        fid: fid.to_string(),
        url: node.url(),
        public_url: node.public_url.clone(),
        count,
        error: String::default(),
    };
    Ok(Json(assignment))
}

pub async fn lookup_handler(
    State(ctx): State<DirectoryState>,
    FormOrJson(request): FormOrJson<LookupRequest>,
) -> Result<Json<Lookup>, VolumeError> {
    if request.volume_id.is_empty() {
        return Err(VolumeError::String("volume_id can't be empty".to_string()));
    }
    let mut volume_id = request.volume_id;
    if let Some(idx) = volume_id.rfind(',') {
        volume_id = volume_id[..idx].to_string();
    }
    let mut locations = vec![];
    let data_nodes = ctx
        .topology
        .lookup(
            &request.collection.unwrap_or_default(),
            volume_id.parse::<u32>()?,
        )
        .await;
    match data_nodes {
        Some(nodes) => {
            for dn in nodes.iter() {
                locations.push(Location {
                    url: dn.url(),
                    public_url: dn.public_url.clone(),
                });
            }

            let lookup = Lookup {
                volume_id,
                locations,
                error: FastStr::default(),
            };
            Ok(Json(lookup))
        }
        None => Err(VolumeError::String("cannot find any locations".to_string())),
    }
}

pub async fn dir_status_handler(State(ctx): State<DirectoryState>) -> Json<Topology> {
    let topology = ctx.topology.topology();
    Json(topology)
}

pub async fn cluster_status_handler(State(ctx): State<DirectoryState>) -> Json<ClusterStatus> {
    let is_leader = ctx.topology.is_leader().await;
    let leader = ctx
        .topology
        .current_leader_address()
        .await
        .unwrap_or_default();
    let peers = ctx.topology.peers().await;

    let status = ClusterStatus {
        is_leader,
        leader,
        peers,
    };
    Json(status)
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        sync::Arc,
        time::Duration,
    };

    use axum::{body::Body, extract::DefaultBodyLimit, http::Request, routing::get, Router};
    use faststr::FastStr;
    use http_body_util::BodyExt as _;
    use hyper_util::{client::legacy::Client, rt::TokioExecutor};
    use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
    use tracing::{info_span, Instrument};
    use turmoil::Builder;

    use crate::{
        directory::{
            api::{assign_handler, cluster_status_handler, dir_status_handler, lookup_handler},
            DirectoryState,
        },
        topology::volume_grow::VolumeGrowth,
        util::{
            args::{MasterOptions, RaftOptions},
            connector,
            http::default_handler,
        },
    };

    #[test]
    pub fn test_master_api() {
        let addr = (IpAddr::from(Ipv4Addr::UNSPECIFIED), 9333);

        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(100))
            .enable_tokio_io()
            .build();

        let topo = crate::topology::tests::setup_topo();
        let topo = Arc::new(topo);
        let options = MasterOptions {
            ip: FastStr::new("127.0.0.1"),
            port: 9333,
            meta_path: FastStr::new("./"),
            pulse: 5,
            volume_size_limit_mb: 30000,
            default_replication: FastStr::new("000"),
            raft: RaftOptions { peers: vec![] },
        };
        let options = Arc::new(options);

        let ctx = DirectoryState {
            topology: topo,
            volume_grow: VolumeGrowth {},
            options,
        };

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

        sim.host("server", move || {
            let router = http_router.clone();
            async move {
                let listener = turmoil::net::TcpListener::bind(addr).await?;
                loop {
                    let (tcp_stream, _remote_addr) = listener.accept().await?;
                    let tcp_stream = hyper_util::rt::TokioIo::new(tcp_stream);

                    let hyper_service =
                        hyper_util::service::TowerToHyperService::new(router.clone());

                    let result = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(tcp_stream, hyper_service)
                        .await;
                    if result.is_err() {
                        // This error only appears when the client doesn't send a request and
                        // terminate the connection.
                        //
                        // If client sends one request then terminate connection whenever, it
                        // doesn't appear.
                        break;
                    }
                }

                Ok(())
            }
            .instrument(info_span!("server"))
        });

        sim.client(
            "client",
            async move {
                let client = Client::builder(TokioExecutor::new()).build(connector::connector());

                let mut request = Request::new(Body::empty());
                *request.uri_mut() = hyper::Uri::from_static("http://server:9333/dir/assign");
                let res = client.request(request).await?;

                let (parts, body) = res.into_parts();
                let body = body.collect().await?.to_bytes();
                let res = hyper::Response::from_parts(parts, body);

                println!("Got response: {:?}", res);

                Ok(())
            }
            .instrument(info_span!("client")),
        );

        sim.run().unwrap();
    }
}
