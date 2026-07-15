use super::{binary_response_limited, decode_binary_limited, parse_group_key};
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use rustqueue_consensus::{
    ClusterRuntime, FederationChannelForward, FederationFetchForward, FederationMigrationForward,
    FederationReadyForward, FederationReleaseForward, FederationTouchForward,
    FederationWriteForward, MigrationReplicaStatusResponse, INTERNAL_CATALOG_FRAME_BYTES,
    INTERNAL_FETCH_RESPONSE_BYTES, INTERNAL_SMALL_FRAME_BYTES, INTERNAL_WRITE_FRAME_BYTES,
    INTERNAL_WRITE_RESPONSE_BYTES,
};
use std::sync::Arc;

pub(super) fn routes() -> Router<Arc<ClusterRuntime>> {
    let small = INTERNAL_SMALL_FRAME_BYTES;
    Router::new()
        .route(
            "/federation/root/snapshot",
            post(root_snapshot).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/federation/catalog/snapshot",
            post(catalog_snapshot).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/federation/catalog/topics/{topic}",
            post(catalog_topic).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/federation/data/write",
            post(write).layer(DefaultBodyLimit::max(INTERNAL_WRITE_FRAME_BYTES)),
        )
        .route(
            "/federation/data/fetch",
            post(fetch).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/federation/data/ready",
            post(ready).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/federation/data/touch",
            post(touch).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/federation/data/release",
            post(release).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/federation/data/channel",
            post(channel).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/federation/data/migration",
            post(migration).layer(DefaultBodyLimit::max(INTERNAL_CATALOG_FRAME_BYTES)),
        )
        .route(
            "/federation/migration/groups/{group}/status",
            post(group_status).layer(DefaultBodyLimit::max(small)),
        )
}

async fn root_snapshot(
    State(runtime): State<Arc<ClusterRuntime>>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let _: () = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    binary_response_limited(
        &runtime.root_snapshot_routed_local().await,
        INTERNAL_CATALOG_FRAME_BYTES,
    )
}

async fn catalog_snapshot(
    State(runtime): State<Arc<ClusterRuntime>>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let _: () = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    binary_response_limited(
        &runtime.catalog_snapshot_routed_local().await,
        INTERNAL_CATALOG_FRAME_BYTES,
    )
}

async fn catalog_topic(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(topic): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let _: () = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    binary_response_limited(
        &runtime.catalog_topic_routed_local(&topic).await,
        INTERNAL_CATALOG_FRAME_BYTES,
    )
}

async fn write(
    State(runtime): State<Arc<ClusterRuntime>>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: FederationWriteForward = decode_binary_limited(&body, INTERNAL_WRITE_FRAME_BYTES)?;
    binary_response_limited(
        &runtime.forwarded_write_local(request).await,
        INTERNAL_WRITE_RESPONSE_BYTES,
    )
}

async fn fetch(
    State(runtime): State<Arc<ClusterRuntime>>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: FederationFetchForward = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    binary_response_limited(
        &runtime.forwarded_fetch_local(request).await,
        INTERNAL_FETCH_RESPONSE_BYTES,
    )
}

async fn ready(
    State(runtime): State<Arc<ClusterRuntime>>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: FederationReadyForward = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    binary_response_limited(
        &runtime.forwarded_ready_local(request).await,
        INTERNAL_SMALL_FRAME_BYTES,
    )
}

async fn touch(
    State(runtime): State<Arc<ClusterRuntime>>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: FederationTouchForward = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    binary_response_limited(
        &runtime.forwarded_touch_local(request).await,
        INTERNAL_SMALL_FRAME_BYTES,
    )
}

async fn release(
    State(runtime): State<Arc<ClusterRuntime>>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: FederationReleaseForward =
        decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    binary_response_limited(
        &runtime.forwarded_release_local(request).await,
        INTERNAL_SMALL_FRAME_BYTES,
    )
}

async fn channel(
    State(runtime): State<Arc<ClusterRuntime>>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: FederationChannelForward =
        decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    binary_response_limited(
        &runtime.forwarded_channel_local(request).await,
        INTERNAL_WRITE_RESPONSE_BYTES,
    )
}

async fn migration(
    State(runtime): State<Arc<ClusterRuntime>>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: FederationMigrationForward =
        decode_binary_limited(&body, INTERNAL_CATALOG_FRAME_BYTES)?;
    binary_response_limited(
        &runtime.forwarded_migration_local(request).await,
        INTERNAL_CATALOG_FRAME_BYTES,
    )
}

async fn group_status(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let _: () = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    let response: MigrationReplicaStatusResponse = runtime
        .migration_replica_status_local(parse_group_key(&group)?)
        .await;
    binary_response_limited(&response, INTERNAL_SMALL_FRAME_BYTES)
}
