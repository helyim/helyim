use std::{collections::HashMap, ops::Deref};

use faststr::FastStr;
use futures::executor::block_on;
use ginepro::LoadBalancedChannel;
use helyim_common::{grpc_port, parser::parse_host_port};
use helyim_proto::{
    directory::helyim_client::HelyimClient, volume::volume_server_client::VolumeServerClient,
};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tonic::Status;
use tracing::info;

static GRPC_CLIENT_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

type VolumeServerClientMap = HashMap<FastStr, VolumeServerClient<LoadBalancedChannel>>;
static VOLUME_SERVER_CLIENTS: Lazy<VolumeServerClientMap> = Lazy::new(HashMap::new);

pub fn volume_server_client(
    addr: &str,
) -> Result<&mut VolumeServerClient<LoadBalancedChannel>, Status> {
    let clients =
        VOLUME_SERVER_CLIENTS.deref() as *const VolumeServerClientMap as *mut VolumeServerClientMap;
    match unsafe { (*clients).get_mut(addr) } {
        Some(client) => Ok(client),
        None => {
            let (ip, port) =
                parse_host_port(addr).map_err(|err| Status::unavailable(err.to_string()))?;
            let grpc_port = grpc_port(port);

            let channel = block_on(LoadBalancedChannel::builder((ip.clone(), grpc_port)).channel())
                .map_err(|err| Status::unavailable(err.to_string()))?;
            let client = VolumeServerClient::new(channel);
            info!("create volume server client success, addr: {ip}:{grpc_port}");

            let _lock = GRPC_CLIENT_LOCK.lock();
            // WARN: addr is not the grpc addr
            let client = unsafe { (*clients).entry(FastStr::new(addr)).or_insert(client) };
            Ok(client)
        }
    }
}

type HelyimClientMap = HashMap<FastStr, HelyimClient<LoadBalancedChannel>>;
static HELYIM_CLIENTS: Lazy<HelyimClientMap> = Lazy::new(HashMap::new);

pub fn helyim_client(addr: &str) -> Result<&mut HelyimClient<LoadBalancedChannel>, Status> {
    let clients = HELYIM_CLIENTS.deref() as *const HelyimClientMap as *mut HelyimClientMap;
    match unsafe { (*clients).get_mut(addr) } {
        Some(client) => Ok(client),
        None => {
            let (ip, port) =
                parse_host_port(addr).map_err(|err| Status::unavailable(err.to_string()))?;
            let grpc_port = grpc_port(port);

            let channel = block_on(LoadBalancedChannel::builder((ip.clone(), grpc_port)).channel())
                .map_err(|err| Status::unavailable(err.to_string()))?;
            let client = HelyimClient::new(channel);

            info!("create helyim client success, addr: {ip}:{grpc_port}");

            let _lock = GRPC_CLIENT_LOCK.lock();
            // WARN: addr is not the grpc addr
            let client = unsafe { (*clients).entry(FastStr::new(addr)).or_insert(client) };
            Ok(client)
        }
    }
}
