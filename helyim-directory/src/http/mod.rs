mod extractor;
use std::sync::Arc;

use axum::{Json, extract::State};
pub use extractor::require_leader;
use faststr::FastStr;
use helyim_common::{http::FormOrJson, operation::ClusterStatus};
use helyim_topology::{
    Topology, TopologyError, TopologyRef, node::Node, volume_grow::VolumeGrowth,
};
use tracing::debug;

use crate::{
    MasterOptions,
    operation::{AssignRequest, Assignment, Location, Lookup, LookupRequest},
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
) -> Result<Json<Assignment>, TopologyError> {
    let count = match request.count {
        Some(n) if n > 1 => n as u64,
        _ => 1,
    };
    let writable_volume_count = request.writable_volume_count.unwrap_or_default();
    let option = request.volume_grow_option(&state.options.default_replication)?;

    if !state.topology.has_writable_volume(&option).await {
        if state.topology.free_space() <= 0 {
            debug!("no free volumes");
            return Err(TopologyError::NoFreeSpace("no free volumes".to_string()));
        }
        state
            .volume_grow
            .automatic_grow_by_type(
                &option,
                state.topology.as_ref(),
                writable_volume_count as usize,
            )
            .await?;
    }
    let (fid, count, node) = state.topology.pick_for_write(count, &option).await?;
    let assignment = Assignment {
        fid: FastStr::new(fid.to_string()),
        url: node.url(),
        public_url: node.public_url.clone(),
        count,
        error: FastStr::empty(),
    };
    Ok(Json(assignment))
}

pub async fn lookup_handler(
    State(state): State<DirectoryState>,
    FormOrJson(request): FormOrJson<LookupRequest>,
) -> Result<Json<Lookup>, TopologyError> {
    if request.volume_id.is_empty() {
        return Err(TopologyError::String(
            "volume_id can't be empty".to_string(),
        ));
    }
    let mut volume_id: &str = &request.volume_id;
    if let Some(idx) = volume_id.rfind(',') {
        volume_id = &volume_id[..idx];
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

            Ok(Json(Lookup::ok(volume_id, locations)))
        }
        None => Err(TopologyError::String(
            "cannot find any locations".to_string(),
        )),
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

    use axum::{Router, body::Body, http::Request, routing::get};
    use faststr::FastStr;
    use futures::executor::block_on;
    use helyim_common::{connector, http::default_handler};
    use helyim_topology::{tests, volume_grow::VolumeGrowth};
    use http_body_util::BodyExt as _;
    use hyper::Method;
    use hyper_util::{
        client::legacy::{Client, connect::Connect},
        rt::TokioExecutor,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use tracing::{Instrument, info_span};
    use turmoil::Builder;

    use crate::{
        MasterOptions,
        args::RaftOptions,
        http::{
            DirectoryState, assign_handler, cluster_status_handler, dir_status_handler,
            lookup_handler,
        },
        operation::Assignment,
    };

    #[test]
    pub fn test_master_api() {
        let addr = (IpAddr::from(Ipv4Addr::UNSPECIFIED), 9333);

        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(100))
            .enable_tokio_io()
            .build();

        let topo = block_on(tests::setup_topo());

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

                // the client should heartbeat to server, then request lookup api
                // let _: Lookup = http_request(
                //     &client,
                //     "http://server:9333/dir/lookup?volumeId=1",
                //     Method::GET,
                // )
                // .await;

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
