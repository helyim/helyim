use std::{sync::Arc, time::Duration};

use async_stream::stream;
use axum::{routing::get, Router};
use faststr::FastStr;
use futures::{channel::mpsc::unbounded, lock::Mutex, StreamExt};
use helyim_proto::{helyim_client::HelyimClient, HeartbeatResponse};
use tokio::task::JoinHandle;
use tonic::{transport::Channel, Streaming};
use tracing::{error, info};

use crate::{
    errors::Result,
    operation::{looker_loop, Looker, LookerEventTx},
    rt_spawn,
    storage::{
        api::{assign_volume_handler, fallback_handler, status_handler, StorageContext},
        needle_map::NeedleMapType,
        store::Store,
    },
    STOP_INTERVAL,
};

pub struct StorageServer {
    host: FastStr,
    port: u16,
    pub master_node: FastStr,
    pub pulse_seconds: i64,
    pub data_center: FastStr,
    pub rack: FastStr,
    pub store: Arc<Mutex<Store>>,
    pub needle_map_type: NeedleMapType,
    pub read_redirect: bool,
    handles: Vec<JoinHandle<()>>,

    shutdown: async_broadcast::Sender<()>,
}

impl StorageServer {
    pub fn new(
        ip_bind: &str,
        ip: &str,
        port: u16,
        public_url: &str,
        folders: Vec<String>,
        max_counts: Vec<i64>,
        needle_map_type: NeedleMapType,
        master_node: &str,
        pulse_seconds: i64,
        data_center: &str,
        rack: &str,
        read_redirect: bool,
    ) -> Result<StorageServer> {
        let (shutdown, shutdown_rx) = async_broadcast::broadcast(16);

        let store = Store::new(
            ip,
            port,
            public_url,
            folders,
            max_counts,
            needle_map_type,
            shutdown_rx,
        )?;
        Ok(StorageServer {
            host: FastStr::new(ip_bind),
            port,
            master_node: FastStr::new(master_node),
            pulse_seconds,
            data_center: FastStr::new(data_center),
            rack: FastStr::new(rack),
            needle_map_type,
            read_redirect,
            store: Arc::new(Mutex::new(store)),
            handles: vec![],
            shutdown,
        })
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.shutdown.broadcast(()).await?;

        let mut interval = tokio::time::interval(STOP_INTERVAL);

        loop {
            self.handles.retain(|handle| !handle.is_finished());
            if self.handles.is_empty() {
                break;
            }
            interval.tick().await;
        }

        Ok(())
    }

    fn grpc_addr(&self) -> Result<String> {
        match self.master_node.rfind(':') {
            Some(idx) => {
                let port = self.master_node[idx + 1..].parse::<u16>()?;
                Ok(format!("http://{}:{}", &self.master_node[..idx], port + 1))
            }
            None => Ok(self.master_node.to_string()),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let store = self.store.clone();
        let needle_map_type = self.needle_map_type;
        let read_redirect = self.read_redirect;
        let pulse_seconds = self.pulse_seconds as u64;

        let client = HelyimClient::connect(self.grpc_addr()?).await?;

        let (looker_tx, looker_rx) = unbounded();
        let looker = Looker::new(client.clone());
        self.handles.push(rt_spawn(looker_loop(looker, looker_rx)));

        let ctx = StorageContext {
            store,
            needle_map_type,
            read_redirect,
            pulse_seconds,
            looker: LookerEventTx::new(looker_tx),
        };

        self.handles.push(
            start_heartbeat(
                self.store.clone(),
                client,
                self.pulse_seconds,
                self.shutdown.new_receiver(),
            )
            .await,
        );

        // http server
        let addr_str = format!("{}:{}", self.host, self.port);
        let addr = addr_str.parse()?;
        let mut shutdown_rx = self.shutdown.new_receiver();

        self.handles.push(rt_spawn(async move {
            let app = Router::new()
                .route("/status", get(status_handler))
                .route("/admin/assign_volume", get(assign_volume_handler))
                .fallback(fallback_handler)
                .with_state(ctx);

            let server = hyper::Server::bind(&addr).serve(app.into_make_service());
            let graceful = server.with_graceful_shutdown(async {
                let _ = shutdown_rx.recv().await;
            });
            info!("storage server starting up.");
            match graceful.await {
                Ok(()) => info!("storage server shutting down gracefully."),
                Err(e) => error!("storage server stop failed, {}", e),
            }
        }));

        Ok(())
    }
}

async fn start_heartbeat(
    store: Arc<Mutex<Store>>,
    mut client: HelyimClient<Channel>,
    pulse_seconds: i64,
    mut shutdown_rx: async_broadcast::Receiver<()>,
) -> JoinHandle<()> {
    rt_spawn(async move {
        'next_heartbeat: loop {
            tokio::select! {
                stream = heartbeat_stream(
                    store.clone(),
                    &mut client,
                    pulse_seconds,
                    shutdown_rx.new_receiver(),
                ) => {
                    match stream {
                        Ok(mut stream) => {
                            info!("heartbeat starting up success");
                            while let Some(response) = stream.next().await {
                                match response {
                                    Ok(response) => store.lock().await.volume_size_limit = response.volume_size_limit,
                                    Err(err) => {
                                        error!("send heartbeat error: {}", err.message());
                                        tokio::time::sleep(STOP_INTERVAL * 2).await;
                                        continue 'next_heartbeat;
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            error!("heartbeat starting up failed: {err}");
                            tokio::time::sleep(STOP_INTERVAL * 2).await;
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("stopping heartbeat.");
                    return;
                }
            }
        }
    })
}

async fn heartbeat_stream(
    store: Arc<Mutex<Store>>,
    client: &mut HelyimClient<Channel>,
    pulse_seconds: i64,
    mut shutdown_rx: async_broadcast::Receiver<()>,
) -> Result<Streaming<HeartbeatResponse>> {
    let mut interval = tokio::time::interval(Duration::from_secs(pulse_seconds as u64));

    let request_stream = stream! {
        loop {
            tokio::select! {
                // to avoid server side got `channel closed` error
                _ = shutdown_rx.recv() => {
                    break;
                }
                _ = interval.tick() => {
                    let heartbeat = store.lock().await.collect_heartbeat();
                    yield heartbeat
                }
            }
        }
    };
    let response = client.heartbeat(request_stream).await?;
    Ok(response.into_inner())
}
