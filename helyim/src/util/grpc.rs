use std::{collections::HashMap, ops::Deref};

use faststr::FastStr;
use helyim_proto::{helyim_client::HelyimClient, volume_server_client::VolumeServerClient};
use once_cell::sync::Lazy;
use tonic::transport::Channel;

use crate::errors::Result;

type VolumeServerClientMap = HashMap<FastStr, VolumeServerClient<Channel>>;
static VOLUME_SERVER_CLIENTS: Lazy<VolumeServerClientMap> = Lazy::new(HashMap::new);

pub async fn volume_server_client(addr: &str) -> Result<&mut VolumeServerClient<Channel>> {
    let clients =
        VOLUME_SERVER_CLIENTS.deref() as *const VolumeServerClientMap as *mut VolumeServerClientMap;
    unsafe {
        match (*clients).get_mut(addr) {
            Some(client) => Ok(client),
            None => {
                let client = VolumeServerClient::connect(addr.to_string()).await?;
                let client = (*clients).entry(FastStr::new(addr)).or_insert(client);
                Ok(client)
            }
        }
    }
}

type HelyimClientMap = HashMap<FastStr, HelyimClient<Channel>>;
static HELYIM_CLIENTS: Lazy<HelyimClientMap> = Lazy::new(HashMap::new);

pub async fn helyim_client(addr: &str) -> Result<&mut HelyimClient<Channel>> {
    let clients = HELYIM_CLIENTS.deref() as *const HelyimClientMap as *mut HelyimClientMap;
    unsafe {
        match (*clients).get_mut(addr) {
            Some(client) => Ok(client),
            None => {
                let client = HelyimClient::connect(addr.to_string()).await?;
                let client = (*clients).entry(FastStr::new(addr)).or_insert(client);
                Ok(client)
            }
        }
    }
}
