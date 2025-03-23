use helyim_proto::directory::{VolumeEcShardInformationMessage, VolumeShortInformationMessage};
use kanal::{AsyncReceiver, AsyncSender, unbounded_async};
use tracing::error;

#[derive(Clone)]
pub struct DeltaVolumeInfoSender {
    pub new_volumes_tx: AsyncSender<VolumeShortInformationMessage>,
    pub deleted_volumes_tx: AsyncSender<VolumeShortInformationMessage>,
    pub new_ec_shards_tx: AsyncSender<VolumeEcShardInformationMessage>,
    pub deleted_ec_shards_tx: AsyncSender<VolumeEcShardInformationMessage>,
}

impl DeltaVolumeInfoSender {
    pub async fn add_volume(&self, message: VolumeShortInformationMessage) {
        if let Err(err) = self.new_volumes_tx.send(message).await {
            error!("add delta volume info error: {err}");
        }
    }

    pub async fn delete_volume(&self, message: VolumeShortInformationMessage) {
        if let Err(err) = self.deleted_volumes_tx.send(message).await {
            error!("delete delta volume info error: {err}");
        }
    }

    pub async fn add_ec_shard(&self, message: VolumeEcShardInformationMessage) {
        if let Err(err) = self.new_ec_shards_tx.send(message).await {
            error!("add delta ec shard info error: {err}");
        }
    }

    pub async fn delete_ec_shard(&self, message: VolumeEcShardInformationMessage) {
        if let Err(err) = self.deleted_ec_shards_tx.send(message).await {
            error!("delete delta ec shard info error: {err}");
        }
    }
}

#[derive(Clone)]
pub struct DeltaVolumeInfoReceiver {
    pub new_volumes_rx: AsyncReceiver<VolumeShortInformationMessage>,
    pub deleted_volumes_rx: AsyncReceiver<VolumeShortInformationMessage>,
    pub new_ec_shards_rx: AsyncReceiver<VolumeEcShardInformationMessage>,
    pub deleted_ec_shards_rx: AsyncReceiver<VolumeEcShardInformationMessage>,
}

pub fn delta_volume_channel() -> (DeltaVolumeInfoSender, DeltaVolumeInfoReceiver) {
    let (new_volumes_tx, new_volumes_rx) = unbounded_async();
    let (deleted_volumes_tx, deleted_volumes_rx) = unbounded_async();
    let (new_ec_shards_tx, new_ec_shards_rx) = unbounded_async();
    let (deleted_ec_shards_tx, deleted_ec_shards_rx) = unbounded_async();

    (
        DeltaVolumeInfoSender {
            new_volumes_tx,
            deleted_volumes_tx,
            new_ec_shards_tx,
            deleted_ec_shards_tx,
        },
        DeltaVolumeInfoReceiver {
            new_volumes_rx,
            deleted_volumes_rx,
            new_ec_shards_rx,
            deleted_ec_shards_rx,
        },
    )
}
