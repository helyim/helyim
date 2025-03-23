use std::result::Result as StdResult;

use axum::extract::State;
use helyim_client::volume_server_client;
use helyim_ec::EcVolumeError;
use helyim_proto::volume::{
    VolumeEcShardsGenerateRequest, VolumeEcShardsRebuildRequest, VolumeEcShardsToVolumeRequest,
};
use tonic::Status;

use crate::http::{StorageState, extractor::ErasureCodingExtractor};

pub async fn generate_ec_shards_handler(
    State(state): State<StorageState>,
    extractor: ErasureCodingExtractor,
) -> StdResult<(), EcVolumeError> {
    generate_ec_shards(state, extractor)
        .await
        .map_err(|err| EcVolumeError::Box(Box::new(err)))
}

pub async fn generate_ec_shards(
    state: StorageState,
    extractor: ErasureCodingExtractor,
) -> StdResult<(), Status> {
    let client = volume_server_client(&state.store.public_url)?;
    let request = VolumeEcShardsGenerateRequest {
        volume_id: extractor.query.volume,
        collection: extractor.query.collection.unwrap_or_default().to_string(),
    };
    let _ = client.volume_ec_shards_generate(request).await?;
    Ok(())
}

pub async fn generate_volume_from_ec_shards_handler(
    State(state): State<StorageState>,
    extractor: ErasureCodingExtractor,
) -> StdResult<(), EcVolumeError> {
    generate_volume_from_ec_shards(state, extractor)
        .await
        .map_err(|err| EcVolumeError::Box(Box::new(err)))
}

pub async fn generate_volume_from_ec_shards(
    state: StorageState,
    extractor: ErasureCodingExtractor,
) -> StdResult<(), Status> {
    let client = volume_server_client(&state.store.public_url)?;
    let request = VolumeEcShardsToVolumeRequest {
        volume_id: extractor.query.volume,
        collection: extractor.query.collection.unwrap_or_default().to_string(),
    };
    let _ = client.volume_ec_shards_to_volume(request).await?;
    Ok(())
}

pub async fn rebuild_missing_ec_shards_handler(
    State(state): State<StorageState>,
    extractor: ErasureCodingExtractor,
) -> StdResult<(), EcVolumeError> {
    rebuild_missing_ec_shards(state, extractor)
        .await
        .map_err(|err| EcVolumeError::Box(Box::new(err)))
}

pub async fn rebuild_missing_ec_shards(
    state: StorageState,
    extractor: ErasureCodingExtractor,
) -> StdResult<(), Status> {
    let client = volume_server_client(&state.store.public_url)?;
    let request = VolumeEcShardsRebuildRequest {
        volume_id: extractor.query.volume,
        collection: extractor.query.collection.unwrap_or_default().to_string(),
    };
    let _ = client.volume_ec_shards_rebuild(request).await?;
    Ok(())
}
