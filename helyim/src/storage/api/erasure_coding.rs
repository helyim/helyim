use std::result::Result as StdResult;

use axum::extract::State;
use helyim_proto::volume::{
    VolumeEcShardsGenerateRequest, VolumeEcShardsRebuildRequest, VolumeEcShardsToVolumeRequest,
};

use crate::{
    storage::{api::StorageState, erasure_coding::EcVolumeError},
    util::{grpc::volume_server_client, http::extractor::ErasureCodingExtractor},
};

pub async fn generate_ec_shards_handler(
    State(state): State<StorageState>,
    extractor: ErasureCodingExtractor,
) -> StdResult<(), EcVolumeError> {
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
    let client = volume_server_client(&state.store.public_url)?;
    let request = VolumeEcShardsRebuildRequest {
        volume_id: extractor.query.volume,
        collection: extractor.query.collection.unwrap_or_default().to_string(),
    };
    let _ = client.volume_ec_shards_rebuild(request).await?;
    Ok(())
}
