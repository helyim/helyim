use std::{sync::Arc, time::Duration};

use async_stream::stream;
use futures::{channel::mpsc::unbounded, lock::Mutex, StreamExt};
use helyim_proto::{helyim_client::HelyimClient, HeartbeatResponse};
use tokio::{sync::broadcast, task::JoinHandle};
use tonic::Streaming;
use tracing::{error, info};

use crate::{
    errors::Result,
    operation::Looker,
    rt_spawn,
    storage::{
        api::{handle_http_request, MakeHttpContext, StorageContext},
        needle_map::NeedleMapType,
        store::Store,
    },
    STOP_INTERVAL,
};

pub struct StorageServer {
    host: String,
    port: u16,
    pub master_node: String,
    pub pulse_seconds: i64,
    pub data_center: String,
    pub rack: String,
    pub store: Arc<Mutex<Store>>,
    pub needle_map_type: NeedleMapType,
    pub read_redirect: bool,
    handles: Vec<JoinHandle<()>>,

    shutdown: broadcast::Sender<()>,
}

impl StorageServer {
    #[allow(clippy::too_many_arguments)]
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
        _white_list: Vec<String>,
        read_redirect: bool,
    ) -> Result<StorageServer> {
        let (shutdown, _) = broadcast::channel(16);

        let store = Store::new(
            ip,
            port,
            public_url,
            folders,
            max_counts,
            needle_map_type,
            shutdown.clone(),
        )?;
        Ok(StorageServer {
            host: ip_bind.to_string(),
            port,
            master_node: master_node.to_string(),
            pulse_seconds,
            data_center: data_center.to_string(),
            rack: rack.to_string(),
            needle_map_type,
            read_redirect,
            store: Arc::new(Mutex::new(store)),
            handles: vec![],
            shutdown,
        })
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.shutdown.send(())?;

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

    fn grpc_addr(&self) -> String {
        let idx = self.master_node.rfind(':').unwrap();
        let port = self.master_node[idx + 1..].parse::<u16>().unwrap();

        format!("http://{}:{}", &self.master_node[..idx], port + 1)
    }

    pub async fn start(&mut self) -> Result<()> {
        let (sender, receiver) = unbounded();
        let looker = Arc::new(Mutex::new(Looker::new(&self.master_node)));

        let store = self.store.clone();
        let needle_map_type = self.needle_map_type;
        let read_redirect = self.read_redirect;
        let pulse_seconds = self.pulse_seconds as u64;
        let master_node = self.master_node.clone();

        let ctx = StorageContext {
            store,
            needle_map_type,
            read_redirect,
            pulse_seconds,
            master_node,
            looker: looker.clone(),
        };
        self.handles.push(handle_http_request(ctx, receiver));

        let heartbeat_handle = start_heartbeat(
            self.store.clone(),
            self.grpc_addr(),
            self.pulse_seconds,
            self.shutdown.subscribe(),
        )
        .await;

        // http server
        let addr_str = format!("{}:{}", self.host, self.port);
        let addr = addr_str.parse()?;
        let mut shutdown_rx = self.shutdown.subscribe();

        let http_handle = rt_spawn(async move {
            // let app = Router::new()
            //     .route("/status", get(status_handler).post(status_handler))
            //     .route("/admin/assign_volume",
            // get(assign_volume_handler).post(assign_volume_handler))
            //     .fallback(default_handler)
            //     .with_state(ctx);

            let server = hyper::Server::bind(&addr).serve(MakeHttpContext::new(sender));
            let graceful = server.with_graceful_shutdown(async {
                let _ = shutdown_rx.recv().await;
            });
            info!("storage server starting up.");
            match graceful.await {
                Ok(()) => info!("storage server shutting down gracefully."),
                Err(e) => error!("storage server stop failed, {}", e),
            }
        });

        self.handles.push(http_handle);
        self.handles.push(heartbeat_handle);

        Ok(())
    }
}

async fn start_heartbeat(
    store: Arc<Mutex<Store>>,
    master_node: String,
    pulse_seconds: i64,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> JoinHandle<()> {
    rt_spawn(async move {
        'next_heartbeat: loop {
            match heartbeat_stream(
                store.clone(),
                master_node.clone(),
                pulse_seconds,
                shutdown_rx.resubscribe(),
            )
            .await
            {
                Ok(mut stream) => {
                    info!("start heartbeat success, master: {master_node}");
                    loop {
                        tokio::select! {
                            _ = shutdown_rx.recv() => {
                                info!("stopping heartbeat.");
                                return;
                            }
                            Some(response) = stream.next() => {
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
                    }
                }
                Err(err) => {
                    error!("start heartbeat failed: {err}");
                    tokio::time::sleep(STOP_INTERVAL * 2).await;
                }
            }
        }
    })
}

async fn heartbeat_stream(
    store: Arc<Mutex<Store>>,
    master_node: String,
    pulse_seconds: i64,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<Streaming<HeartbeatResponse>> {
    let mut client = HelyimClient::connect(master_node).await?;
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
    let response = client.send_heartbeat(request_stream).await?;
    Ok(response.into_inner())
}
