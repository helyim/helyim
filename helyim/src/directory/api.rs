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
    State(state): State<DirectoryState>,
    FormOrJson(request): FormOrJson<AssignRequest>,
) -> Result<Json<Assignment>, VolumeError> {
    let count = match request.count {
        Some(n) if n > 1 => n,
        _ => 1,
    };
    let option = request.volume_grow_option(&state.options.default_replication)?;

    if !state.topology.has_writable_volume(&option).await {
        if state.topology.free_space() <= 0 {
            return Err(VolumeError::NoFreeSpace("no free volumes".to_string()));
        }
        state
            .volume_grow
            .grow_by_type(&option, state.topology.as_ref())
            .await?;
    }
    let (fid, count, node) = state.topology.pick_for_write(count, &option).await?;
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
    State(state): State<DirectoryState>,
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
    let data_nodes = state
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

pub async fn dir_status_handler(State(state): State<DirectoryState>) -> Json<Topology> {
    let topology = state.topology.topology();
    Json(topology)
}

pub async fn cluster_status_handler(State(state): State<DirectoryState>) -> Json<ClusterStatus> {
    let is_leader = state.topology.is_leader().await;
    let leader = state
        .topology
        .current_leader_address()
        .await
        .unwrap_or_default();
    let peers = state.topology.peers().await;

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

    use axum::{body::Body, http::Request, routing::get, Router};
    use faststr::FastStr;
    use http_body_util::BodyExt as _;
    use hyper::Method;
    use hyper_util::{
        client::legacy::{connect::Connect, Client},
        rt::TokioExecutor,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use tracing::{info_span, Instrument};
    use turmoil::Builder;

    use crate::{
        directory::{
            api::{assign_handler, cluster_status_handler, dir_status_handler, lookup_handler},
            DirectoryState,
        },
        operation::{lookup::Lookup, Assignment},
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

        let state = DirectoryState {
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
            .with_state(state);

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

                let _: Assignment =
                    http_request(&client, "http://server:9333/dir/assign", Method::GET).await;

                let _: Value =
                    http_request(&client, "http://server:9333/dir/status", Method::GET).await;
                let _: Value =
                    http_request(&client, "http://server:9333/dir/status", Method::POST).await;

                let _: Lookup = http_request(
                    &client,
                    "http://server:9333/dir/lookup?volumeId=1",
                    Method::GET,
                )
                .await;

                Ok(())
            }
            .instrument(info_span!("client")),
        );

        sim.run().unwrap();
    }

    async fn http_request<C, T: Serialize + for<'a> Deserialize<'a>>(
        client: &Client<C, Body>,
        uri: &'static str,
        method: Method,
    ) -> T
    where
        C: Connect + Clone + Send + Sync + 'static,
    {
        let mut request = Request::new(Body::empty());
        *request.method_mut() = method;
        *request.uri_mut() = hyper::Uri::from_static(uri);
        let res = client.request(request).await.unwrap();

        let (parts, body) = res.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        let res = hyper::Response::from_parts(parts, body);

        serde_json::from_slice::<T>(res.body()).unwrap()
    }
}
