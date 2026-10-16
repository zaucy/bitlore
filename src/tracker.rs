use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::local_store::{ImmutableStore, StoreGetData, StoreMatch, StoreMatchResult};
use crate::types::{Address, Context, Fragment, FragmentFlags, Hash, Partition};

/// Default TTL before a peer without heartbeats is considered offline by the tracker.
pub const DEFAULT_PEER_TTL: Duration = Duration::from_secs(90);

// ============================================================================
// Prost / gRPC Wire Messages for `/bitlore.p2p.v1.P2pTracker`
// ============================================================================

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileOwnership {
    /// 16-byte RepositoryId (Partition).
    #[prost(bytes = "vec", tag = "1")]
    pub repository_id: Vec<u8>,
    /// 16-byte file_id (`Address.context`). Zero if announcing repo-level presence only.
    #[prost(bytes = "vec", tag = "2")]
    pub file_id: Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnounceRequest {
    /// Unique `iroh::EndpointId` string of the announcing peer.
    #[prost(string, tag = "1")]
    pub peer_id: String,
    /// JSON-serialized `iroh::EndpointAddr` (contains `EndpointId`, relay URLs, and direct socket addrs).
    #[prost(string, tag = "2")]
    pub endpoint_addr_json: String,
    /// List of `(repository_id, file_id)` pairs this peer holds in its local store.
    #[prost(message, repeated, tag = "3")]
    pub files: Vec<FileOwnership>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnounceResponse {
    #[prost(uint64, tag = "1")]
    pub registered_files: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolveRequest {
    /// 16-byte RepositoryId (Partition).
    #[prost(bytes = "vec", tag = "1")]
    pub repository_id: Vec<u8>,
    /// 16-byte file_id (`Address.context`).
    #[prost(bytes = "vec", tag = "2")]
    pub file_id: Vec<u8>,
    /// Optional `peer_id` of the requesting peer so the tracker excludes the requester itself.
    #[prost(string, tag = "3")]
    pub requester_peer_id: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerEndpointInfo {
    #[prost(string, tag = "1")]
    pub peer_id: String,
    #[prost(string, tag = "2")]
    pub endpoint_addr_json: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolveResponse {
    /// Active peers known to hold `(repository_id, file_id)` (followed by repo-level fallback peers).
    #[prost(message, repeated, tag = "1")]
    pub peers: Vec<PeerEndpointInfo>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatRequest {
    #[prost(string, tag = "1")]
    pub peer_id: String,
    #[prost(string, tag = "2")]
    pub endpoint_addr_json: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatResponse {
    /// `true` if the peer was already known; `false` if the peer should re-announce its inventory.
    #[prost(bool, tag = "1")]
    pub known: bool,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StructuralAddressProto {
    /// 32-byte BLAKE3 hash.
    #[prost(bytes = "vec", tag = "1")]
    pub hash: Vec<u8>,
    /// 16-byte Context (all zeros for structural fragments).
    #[prost(bytes = "vec", tag = "2")]
    pub context: Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryStructuralRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub repository_id: Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    pub addresses: Vec<StructuralAddressProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryStructuralResponse {
    /// `true` for each address in `QueryStructuralRequest.addresses` that is stored on `bitlore-server`.
    #[prost(bool, repeated, tag = "1")]
    pub present: Vec<bool>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutStructuralRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub repository_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub hash: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub context: Vec<u8>,
    #[prost(uint32, tag = "4")]
    pub size_payload: u32,
    #[prost(uint64, tag = "5")]
    pub size_content: u64,
    #[prost(uint32, tag = "6")]
    pub flags: u32,
    #[prost(bytes = "bytes", tag = "7")]
    pub payload: Bytes,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutStructuralResponse {}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetStructuralRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub repository_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub hash: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub context: Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetStructuralResponse {
    #[prost(bool, tag = "1")]
    pub found: bool,
    #[prost(uint32, tag = "2")]
    pub size_payload: u32,
    #[prost(uint64, tag = "3")]
    pub size_content: u64,
    #[prost(uint32, tag = "4")]
    pub flags: u32,
    #[prost(bytes = "bytes", tag = "5")]
    pub payload: Bytes,
}

// ============================================================================
// In-Memory Tracker State (`BitloreP2pTrackerState`)
// ============================================================================

#[derive(Debug, Clone)]
pub struct PeerRecord {
    pub peer_id: String,
    pub endpoint_addr_json: String,
    pub last_heartbeat: Instant,
    pub repositories: HashSet<[u8; 16]>,
}

pub struct BitloreP2pTrackerState {
    peers: DashMap<String, PeerRecord>,
    /// Maps `(repository_id, file_id)` -> set of `peer_id`s holding that file's chunks.
    file_owners: DashMap<([u8; 16], [u8; 16]), HashSet<String>>,
    /// Backing metadata store on `bitlore-server` for structural fragments (`context == 0`).
    metadata_store: Arc<dyn ImmutableStore>,
    peer_ttl: Duration,
}

impl BitloreP2pTrackerState {
    pub fn new(metadata_store: Arc<dyn ImmutableStore>, peer_ttl: Duration) -> Self {
        Self {
            peers: DashMap::new(),
            file_owners: DashMap::new(),
            metadata_store,
            peer_ttl,
        }
    }

    pub fn announce(&self, req: AnnounceRequest) -> u64 {
        let now = Instant::now();
        let mut repos = HashSet::new();
        let mut count = 0u64;

        for ownership in &req.files {
            if let Ok(repo_bytes) = <[u8; 16]>::try_from(ownership.repository_id.as_slice()) {
                repos.insert(repo_bytes);
                if let Ok(file_bytes) = <[u8; 16]>::try_from(ownership.file_id.as_slice())
                    && file_bytes != [0u8; 16]
                {
                    self.file_owners
                        .entry((repo_bytes, file_bytes))
                        .or_default()
                        .insert(req.peer_id.clone());
                    count += 1;
                }
            }
        }

        self.peers
            .entry(req.peer_id.clone())
            .and_modify(|record| {
                if !req.endpoint_addr_json.is_empty() {
                    record.endpoint_addr_json = req.endpoint_addr_json.clone();
                }
                record.last_heartbeat = now;
                record.repositories.extend(repos.iter().copied());
            })
            .or_insert_with(|| PeerRecord {
                peer_id: req.peer_id.clone(),
                endpoint_addr_json: req.endpoint_addr_json.clone(),
                last_heartbeat: now,
                repositories: repos,
            });

        info!(
            peer_id = %req.peer_id,
            registered_files = count,
            total_peers = self.peers.len(),
            total_tracked_files = self.file_owners.len(),
            "Bitlore P2P Tracker: peer announced file ownership"
        );
        count
    }

    pub fn resolve(&self, req: &ResolveRequest) -> Vec<PeerEndpointInfo> {
        let Ok(repo_bytes) = <[u8; 16]>::try_from(req.repository_id.as_slice()) else {
            return Vec::new();
        };
        let file_bytes = <[u8; 16]>::try_from(req.file_id.as_slice()).unwrap_or([0u8; 16]);
        let now = Instant::now();

        let mut seen_peers = HashSet::new();
        let mut exact_candidates = Vec::new();
        let mut fallback_candidates = Vec::new();

        // 1. Exact `(repository_id, file_id)` owners first
        if file_bytes != [0u8; 16]
            && let Some(owners) = self.file_owners.get(&(repo_bytes, file_bytes))
        {
            for peer_id in owners.iter() {
                if peer_id == &req.requester_peer_id {
                    continue;
                }
                if let Some(peer) = self.peers.get(peer_id)
                    && now.duration_since(peer.last_heartbeat) <= self.peer_ttl
                    && seen_peers.insert(peer_id.clone())
                {
                    exact_candidates.push((
                        peer.last_heartbeat,
                        PeerEndpointInfo {
                            peer_id: peer.peer_id.clone(),
                            endpoint_addr_json: peer.endpoint_addr_json.clone(),
                        },
                    ));
                }
            }
        }
        exact_candidates.sort_by(|a, b| b.0.cmp(&a.0));

        // 2. Fallback: other active peers in the same `repository_id`
        for entry in self.peers.iter() {
            let peer = entry.value();
            if peer.peer_id == req.requester_peer_id {
                continue;
            }
            if now.duration_since(peer.last_heartbeat) <= self.peer_ttl
                && peer.repositories.contains(&repo_bytes)
                && seen_peers.insert(peer.peer_id.clone())
            {
                fallback_candidates.push((
                    peer.last_heartbeat,
                    PeerEndpointInfo {
                        peer_id: peer.peer_id.clone(),
                        endpoint_addr_json: peer.endpoint_addr_json.clone(),
                    },
                ));
            }
        }
        fallback_candidates.sort_by(|a, b| b.0.cmp(&a.0));

        let resolved: Vec<PeerEndpointInfo> = exact_candidates
            .into_iter()
            .chain(fallback_candidates)
            .map(|(_, info)| info)
            .collect();

        info!(
            repository_id = %hex::encode(repo_bytes),
            file_id = %hex::encode(file_bytes),
            requester = %req.requester_peer_id,
            matched_peers = resolved.len(),
            "Bitlore P2P Tracker: resolved peer endpoints for file request"
        );
        resolved
    }

    pub fn heartbeat(&self, req: HeartbeatRequest) -> bool {
        if let Some(mut peer) = self.peers.get_mut(&req.peer_id) {
            peer.last_heartbeat = Instant::now();
            if !req.endpoint_addr_json.is_empty() {
                peer.endpoint_addr_json = req.endpoint_addr_json;
            }
            debug!(peer_id = %req.peer_id, "Bitlore P2P Tracker: heartbeat refreshed");
            true
        } else {
            false
        }
    }
}

// ============================================================================
// Tonic gRPC Service Implementation (No build.rs / protoc required)
// ============================================================================

#[derive(Clone)]
pub struct BitloreTrackerGrpcService {
    state: Arc<BitloreP2pTrackerState>,
}

impl BitloreTrackerGrpcService {
    pub fn new(state: Arc<BitloreP2pTrackerState>) -> Self {
        Self { state }
    }

    async fn handle_announce(
        &self,
        request: Request<AnnounceRequest>,
    ) -> Result<Response<AnnounceResponse>, Status> {
        let registered_files = self.state.announce(request.into_inner());
        Ok(Response::new(AnnounceResponse { registered_files }))
    }

    async fn handle_resolve(
        &self,
        request: Request<ResolveRequest>,
    ) -> Result<Response<ResolveResponse>, Status> {
        let peers = self.state.resolve(request.get_ref());
        Ok(Response::new(ResolveResponse { peers }))
    }

    async fn handle_heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let known = self.state.heartbeat(request.into_inner());
        Ok(Response::new(HeartbeatResponse { known }))
    }

    async fn handle_query_structural(
        &self,
        request: Request<QueryStructuralRequest>,
    ) -> Result<Response<QueryStructuralResponse>, Status> {
        let req = request.into_inner();
        let partition = parse_partition(&req.repository_id)?;
        let mut addresses = Vec::with_capacity(req.addresses.len());
        for a in &req.addresses {
            addresses.push(parse_address(&a.hash, &a.context)?);
        }
        let mut results = vec![StoreMatchResult::default(); addresses.len()];
        self.state
            .metadata_store
            .clone()
            .query(partition, &addresses, &mut results)
            .await
            .map_err(|e| Status::internal(format!("metadata store query failed: {e}")))?;

        let present = results
            .into_iter()
            .map(|r| r.match_made == StoreMatch::MatchFull && r.stored_durable)
            .collect();
        Ok(Response::new(QueryStructuralResponse { present }))
    }

    async fn handle_put_structural(
        &self,
        request: Request<PutStructuralRequest>,
    ) -> Result<Response<PutStructuralResponse>, Status> {
        let req = request.into_inner();
        let partition = parse_partition(&req.repository_id)?;
        let address = parse_address(&req.hash, &req.context)?;
        let fragment = Fragment {
            size_payload: req.size_payload,
            size_content: req.size_content,
            flags: req.flags | FragmentFlags::PayloadStoredDurable.bits(),
        };
        self.state
            .metadata_store
            .clone()
            .put(partition, address, fragment, Some(req.payload), false)
            .await
            .map_err(|e| Status::internal(format!("metadata store put failed: {e}")))?;
        Ok(Response::new(PutStructuralResponse {}))
    }

    async fn handle_get_structural(
        &self,
        request: Request<GetStructuralRequest>,
    ) -> Result<Response<GetStructuralResponse>, Status> {
        let req = request.into_inner();
        let partition = parse_partition(&req.repository_id)?;
        let address = parse_address(&req.hash, &req.context)?;
        match self
            .state
            .metadata_store
            .clone()
            .get(partition, address)
            .await
            .and_then(StoreGetData::into_payload)
        {
            Ok((fragment, payload)) => Ok(Response::new(GetStructuralResponse {
                found: true,
                size_payload: fragment.size_payload,
                size_content: fragment.size_content,
                flags: fragment.flags,
                payload,
            })),
            Err(_) => Ok(Response::new(GetStructuralResponse {
                found: false,
                size_payload: 0,
                size_content: 0,
                flags: 0,
                payload: Bytes::new(),
            })),
        }
    }
}

fn parse_partition(bytes: &[u8]) -> Result<Partition, Status> {
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("repository_id must be 16 bytes"))?;
    Ok(Partition::from(&arr))
}

fn parse_address(hash_bytes: &[u8], context_bytes: &[u8]) -> Result<Address, Status> {
    let h: [u8; 32] = hash_bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("hash must be 32 bytes"))?;
    let c: [u8; 16] = context_bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("context must be 16 bytes"))?;
    Ok(Address {
        hash: Hash::from(h),
        context: Context::from(&c),
    })
}

impl tonic::server::NamedService for BitloreTrackerGrpcService {
    const NAME: &'static str = "bitlore.p2p.v1.P2pTracker";
}

impl<B> tonic::codegen::Service<http::Request<B>> for BitloreTrackerGrpcService
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let svc = self.clone();
        Box::pin(async move {
            match req.uri().path() {
                "/bitlore.p2p.v1.P2pTracker/Announce" => {
                    struct AnnounceSvc(BitloreTrackerGrpcService);
                    impl tonic::server::UnaryService<AnnounceRequest> for AnnounceSvc {
                        type Response = AnnounceResponse;
                        type Future = futures::future::BoxFuture<
                            'static,
                            Result<Response<Self::Response>, Status>,
                        >;
                        fn call(&mut self, request: Request<AnnounceRequest>) -> Self::Future {
                            let s = self.0.clone();
                            Box::pin(async move { s.handle_announce(request).await })
                        }
                    }
                    let mut grpc = tonic::server::Grpc::new(
                        tonic_prost::ProstCodec::<AnnounceResponse, AnnounceRequest>::default(),
                    );
                    Ok(grpc.unary(AnnounceSvc(svc), req).await)
                }
                "/bitlore.p2p.v1.P2pTracker/Resolve" => {
                    struct ResolveSvc(BitloreTrackerGrpcService);
                    impl tonic::server::UnaryService<ResolveRequest> for ResolveSvc {
                        type Response = ResolveResponse;
                        type Future = futures::future::BoxFuture<
                            'static,
                            Result<Response<Self::Response>, Status>,
                        >;
                        fn call(&mut self, request: Request<ResolveRequest>) -> Self::Future {
                            let s = self.0.clone();
                            Box::pin(async move { s.handle_resolve(request).await })
                        }
                    }
                    let mut grpc = tonic::server::Grpc::new(
                        tonic_prost::ProstCodec::<ResolveResponse, ResolveRequest>::default(),
                    );
                    Ok(grpc.unary(ResolveSvc(svc), req).await)
                }
                "/bitlore.p2p.v1.P2pTracker/Heartbeat" => {
                    struct HeartbeatSvc(BitloreTrackerGrpcService);
                    impl tonic::server::UnaryService<HeartbeatRequest> for HeartbeatSvc {
                        type Response = HeartbeatResponse;
                        type Future = futures::future::BoxFuture<
                            'static,
                            Result<Response<Self::Response>, Status>,
                        >;
                        fn call(&mut self, request: Request<HeartbeatRequest>) -> Self::Future {
                            let s = self.0.clone();
                            Box::pin(async move { s.handle_heartbeat(request).await })
                        }
                    }
                    let mut grpc = tonic::server::Grpc::new(
                        tonic_prost::ProstCodec::<HeartbeatResponse, HeartbeatRequest>::default(),
                    );
                    Ok(grpc.unary(HeartbeatSvc(svc), req).await)
                }
                "/bitlore.p2p.v1.P2pTracker/QueryStructural" => {
                    struct QuerySvc(BitloreTrackerGrpcService);
                    impl tonic::server::UnaryService<QueryStructuralRequest> for QuerySvc {
                        type Response = QueryStructuralResponse;
                        type Future = futures::future::BoxFuture<
                            'static,
                            Result<Response<Self::Response>, Status>,
                        >;
                        fn call(
                            &mut self,
                            request: Request<QueryStructuralRequest>,
                        ) -> Self::Future {
                            let s = self.0.clone();
                            Box::pin(async move { s.handle_query_structural(request).await })
                        }
                    }
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                        QueryStructuralResponse,
                        QueryStructuralRequest,
                    >::default());
                    Ok(grpc.unary(QuerySvc(svc), req).await)
                }
                "/bitlore.p2p.v1.P2pTracker/PutStructural" => {
                    struct PutSvc(BitloreTrackerGrpcService);
                    impl tonic::server::UnaryService<PutStructuralRequest> for PutSvc {
                        type Response = PutStructuralResponse;
                        type Future = futures::future::BoxFuture<
                            'static,
                            Result<Response<Self::Response>, Status>,
                        >;
                        fn call(&mut self, request: Request<PutStructuralRequest>) -> Self::Future {
                            let s = self.0.clone();
                            Box::pin(async move { s.handle_put_structural(request).await })
                        }
                    }
                    let mut grpc = tonic::server::Grpc::new(
                        tonic_prost::ProstCodec::<PutStructuralResponse, PutStructuralRequest>::default(),
                    )
                    .max_decoding_message_size(64 * 1024 * 1024);
                    Ok(grpc.unary(PutSvc(svc), req).await)
                }
                "/bitlore.p2p.v1.P2pTracker/GetStructural" => {
                    struct GetSvc(BitloreTrackerGrpcService);
                    impl tonic::server::UnaryService<GetStructuralRequest> for GetSvc {
                        type Response = GetStructuralResponse;
                        type Future = futures::future::BoxFuture<
                            'static,
                            Result<Response<Self::Response>, Status>,
                        >;
                        fn call(&mut self, request: Request<GetStructuralRequest>) -> Self::Future {
                            let s = self.0.clone();
                            Box::pin(async move { s.handle_get_structural(request).await })
                        }
                    }
                    let mut grpc = tonic::server::Grpc::new(
                        tonic_prost::ProstCodec::<GetStructuralResponse, GetStructuralRequest>::default(),
                    )
                    .max_encoding_message_size(64 * 1024 * 1024);
                    Ok(grpc.unary(GetSvc(svc), req).await)
                }
                _ => Ok(http::Response::builder()
                    .status(http::StatusCode::OK)
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "12")
                    .body(tonic::body::Body::empty())
                    .unwrap()),
            }
        })
    }
}

/// Spawns the `BitloreTrackerGrpcService` server on `bind_addr`.
pub fn spawn_tracker_grpc_server(
    bind_addr: SocketAddr,
    metadata_store: Arc<dyn ImmutableStore>,
    peer_ttl: Duration,
) -> Arc<BitloreP2pTrackerState> {
    let state = Arc::new(BitloreP2pTrackerState::new(metadata_store, peer_ttl));
    let service = BitloreTrackerGrpcService::new(state.clone());

    crate::spawn_bg(async move {
        info!(%bind_addr, "Starting Bitlore P2P Tracker gRPC service");
        if let Err(err) = tonic::transport::Server::builder()
            .add_service(service)
            .serve(bind_addr)
            .await
        {
            tracing::error!(%bind_addr, error = %err, "Bitlore P2P Tracker gRPC server exited with error");
        }
    });

    state
}

// ============================================================================
// Tonic gRPC Client (`BitloreTrackerClient`)
// ============================================================================

#[derive(Clone)]
pub struct BitloreTrackerClient {
    endpoint_url: String,
    channel: Arc<tokio::sync::OnceCell<tonic::transport::Channel>>,
}

impl BitloreTrackerClient {
    pub fn new(endpoint_url: impl Into<String>) -> Self {
        let mut url = endpoint_url.into();
        if !url.starts_with("http://") && !url.starts_with("https://") {
            url = format!("http://{url}");
        }
        Self {
            endpoint_url: url,
            channel: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    async fn grpc(&self) -> Result<tonic::client::Grpc<tonic::transport::Channel>, Status> {
        let url = self.endpoint_url.clone();
        let ch = self
            .channel
            .get_or_try_init(|| async move {
                let ep = tonic::transport::Endpoint::from_shared(url)
                    .map_err(|e| Status::invalid_argument(format!("invalid tracker url: {e}")))?;
                ep.connect()
                    .await
                    .map_err(|e| Status::unavailable(format!("failed to connect to tracker: {e}")))
            })
            .await?;
        Ok(tonic::client::Grpc::new(ch.clone())
            .max_decoding_message_size(64 * 1024 * 1024)
            .max_encoding_message_size(64 * 1024 * 1024))
    }

    pub async fn announce(&self, req: AnnounceRequest) -> Result<AnnounceResponse, Status> {
        let mut grpc = self.grpc().await?;
        grpc.ready()
            .await
            .map_err(|e| Status::unavailable(format!("tracker not ready: {e}")))?;
        let codec = tonic_prost::ProstCodec::<AnnounceRequest, AnnounceResponse>::default();
        let path = http::uri::PathAndQuery::from_static("/bitlore.p2p.v1.P2pTracker/Announce");
        Ok(grpc.unary(Request::new(req), path, codec).await?.into_inner())
    }

    pub async fn resolve(&self, req: ResolveRequest) -> Result<ResolveResponse, Status> {
        let mut grpc = self.grpc().await?;
        grpc.ready()
            .await
            .map_err(|e| Status::unavailable(format!("tracker not ready: {e}")))?;
        let codec = tonic_prost::ProstCodec::<ResolveRequest, ResolveResponse>::default();
        let path = http::uri::PathAndQuery::from_static("/bitlore.p2p.v1.P2pTracker/Resolve");
        Ok(grpc.unary(Request::new(req), path, codec).await?.into_inner())
    }

    pub async fn heartbeat(&self, req: HeartbeatRequest) -> Result<HeartbeatResponse, Status> {
        let mut grpc = self.grpc().await?;
        grpc.ready()
            .await
            .map_err(|e| Status::unavailable(format!("tracker not ready: {e}")))?;
        let codec = tonic_prost::ProstCodec::<HeartbeatRequest, HeartbeatResponse>::default();
        let path = http::uri::PathAndQuery::from_static("/bitlore.p2p.v1.P2pTracker/Heartbeat");
        Ok(grpc
            .unary(Request::new(req), path, codec)
            .await?
            .into_inner())
    }

    pub async fn query_structural(
        &self,
        req: QueryStructuralRequest,
    ) -> Result<QueryStructuralResponse, Status> {
        let mut grpc = self.grpc().await?;
        grpc.ready()
            .await
            .map_err(|e| Status::unavailable(format!("tracker not ready: {e}")))?;
        let codec =
            tonic_prost::ProstCodec::<QueryStructuralRequest, QueryStructuralResponse>::default();
        let path =
            http::uri::PathAndQuery::from_static("/bitlore.p2p.v1.P2pTracker/QueryStructural");
        Ok(grpc.unary(Request::new(req), path, codec).await?.into_inner())
    }

    pub async fn put_structural(
        &self,
        req: PutStructuralRequest,
    ) -> Result<PutStructuralResponse, Status> {
        let mut grpc = self.grpc().await?;
        grpc.ready()
            .await
            .map_err(|e| Status::unavailable(format!("tracker not ready: {e}")))?;
        let codec =
            tonic_prost::ProstCodec::<PutStructuralRequest, PutStructuralResponse>::default();
        let path = http::uri::PathAndQuery::from_static("/bitlore.p2p.v1.P2pTracker/PutStructural");
        Ok(grpc.unary(Request::new(req), path, codec).await?.into_inner())
    }

    pub async fn get_structural(
        &self,
        req: GetStructuralRequest,
    ) -> Result<GetStructuralResponse, Status> {
        let mut grpc = self.grpc().await?;
        grpc.ready()
            .await
            .map_err(|e| Status::unavailable(format!("tracker not ready: {e}")))?;
        let codec =
            tonic_prost::ProstCodec::<GetStructuralRequest, GetStructuralResponse>::default();
        let path = http::uri::PathAndQuery::from_static("/bitlore.p2p.v1.P2pTracker/GetStructural");
        Ok(grpc.unary(Request::new(req), path, codec).await?.into_inner())
    }
}
