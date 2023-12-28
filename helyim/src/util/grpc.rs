use std::{collections::HashMap, ops::Deref};

use faststr::FastStr;
use futures::executor::block_on;
use ginepro::LoadBalancedChannel;
use helyim_proto::{helyim_client::HelyimClient, volume_server_client::VolumeServerClient};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tracing::info;

use crate::{storage::VolumeError, util::parser::parse_host_port};

pub fn grpc_port(port: u16) -> u16 {
    port + 10000
}

static GRPC_CLIENT_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

type VolumeServerClientMap = HashMap<FastStr, VolumeServerClient<LoadBalancedChannel>>;
static VOLUME_SERVER_CLIENTS: Lazy<VolumeServerClientMap> = Lazy::new(HashMap::new);

pub fn volume_server_client(
    addr: &str,
) -> Result<&mut VolumeServerClient<LoadBalancedChannel>, VolumeError> {
    let clients =
        VOLUME_SERVER_CLIENTS.deref() as *const VolumeServerClientMap as *mut VolumeServerClientMap;
    unsafe {
        match (*clients).get_mut(addr) {
            Some(client) => Ok(client),
            None => {
                let _lock = GRPC_CLIENT_LOCK.lock();

                let (ip, port) = parse_host_port(addr)?;
                let grpc_port = grpc_port(port);

                let channel =
                    block_on(LoadBalancedChannel::builder((ip.clone(), grpc_port)).channel())
                        .map_err(|err| VolumeError::Box(err.into()))?;
                let client = VolumeServerClient::new(channel);
                info!("create volume server tonic client success, addr: {ip}:{grpc_port}");
                // WARN: addr is not the grpc addr
                let client = (*clients).entry(FastStr::new(addr)).or_insert(client);
                Ok(client)
            }
        }
    }
}

type HelyimClientMap = HashMap<FastStr, HelyimClient<LoadBalancedChannel>>;
static HELYIM_CLIENTS: Lazy<HelyimClientMap> = Lazy::new(HashMap::new);

pub fn helyim_client(addr: &str) -> Result<&mut HelyimClient<LoadBalancedChannel>, VolumeError> {
    let clients = HELYIM_CLIENTS.deref() as *const HelyimClientMap as *mut HelyimClientMap;
    unsafe {
        match (*clients).get_mut(addr) {
            Some(client) => Ok(client),
            None => {
                let _lock = GRPC_CLIENT_LOCK.lock();

                let (ip, port) = parse_host_port(addr)?;
                let grpc_port = grpc_port(port);

                let channel =
                    block_on(LoadBalancedChannel::builder((ip.clone(), grpc_port)).channel())
                        .map_err(|err| VolumeError::Box(err.into()))?;
                let client = HelyimClient::new(channel);
                info!("create helyim tonic client success, addr: {ip}:{grpc_port}");
                // WARN: addr is not the grpc addr
                let client = (*clients).entry(FastStr::new(addr)).or_insert(client);
                Ok(client)
            }
        }
    }
}
