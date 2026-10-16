use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use parking_lot::RwLock;
use serde::Deserialize;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;
use zerocopy::IntoBytes;

use crate::local_store::{
    CopyBehavior, ImmutableStore, LocalImmutableStore, LocalMutableStore, MutableStore, StoreMatch,
};
use crate::lore::environment::v1::{
    Config as ProtoEnvConfig, Endpoint as ProtoEndpoint, Environment as ProtoEnvironment,
    EnvironmentGetRequest, EnvironmentGetResponse,
    environment_service_server::{EnvironmentService, EnvironmentServiceServer},
};
use crate::lore::model::v1::{
    Address as ProtoAddress, Branch as ProtoBranch, BranchPoint as ProtoBranchPoint,
    Fragment as ProtoFragment, ItemStatus, Repository as ProtoRepository,
    RevisionItem as ProtoRevisionItem,
};
use crate::lore::repository::v1::{
    RepositoryCreateRequest, RepositoryCreateResponse, RepositoryDeleteRequest,
    RepositoryDeleteResponse, RepositoryGetRequest, RepositoryGetResponse, RepositoryListRequest,
    RepositoryListResponse, RepositoryMetadataGetRequest, RepositoryMetadataGetResponse,
    RepositoryMetadataSetRequest, RepositoryMetadataSetResponse,
    repository_get_request::Query as RepoGetQuery,
    repository_service_server::{RepositoryService, RepositoryServiceServer},
};
use crate::lore::revision::v1::{
    BranchCreateRequest, BranchCreateResponse, BranchDeleteRequest, BranchDeleteResponse,
    BranchGetRequest, BranchGetResponse, BranchListRequest, BranchListResponse,
    BranchMetadataGetRequest, BranchMetadataGetResponse, BranchMetadataSetRequest,
    BranchMetadataSetResponse, BranchPushRequest, BranchPushResponse, RevisionListRequest,
    RevisionListResponse, branch_get_request::Query as BranchGetQuery,
    revision_list_request::Start as RevListStart,
    revision_service_server::{RevisionService, RevisionServiceServer},
};
use crate::lore::storage::v1::{
    CopyRequest, CopyResponse, GetResolvedRequest, GetResolvedResponse, GetResponse,
    MutableCompareAndSwapRequest, MutableCompareAndSwapResponse, MutableLoadRequest,
    MutableLoadResponse, MutableStoreRequest, MutableStoreResponse, PutRequest,
    PutResolvedRequest, PutResolvedResponse, PutResponse, QueryRequest as StorageQueryRequest,
    QueryResponse as StorageQueryResponse, VerifyRequest, VerifyResponse,
    storage_service_server::{StorageService, StorageServiceServer},
};
use crate::p2p_node::BitloreIrohNode;
use crate::peer_store::{BitlorePeerStore, BitlorePeerStoreConfig, PEER_PLUGIN_NAME};
use crate::quic_server::spawn_quic_storage_server;
use crate::store::{BitloreMetadataStore, BitloreStoreConfig};
use crate::tracker::{BitloreTrackerClient, spawn_tracker_grpc_server};
use crate::types::{
    Address, BranchId, Context, Fragment, FragmentFlags, Hash, KeyType, MetadataBlob, Partition,
    RepositoryId, SALT_LORE, StateDataView, hash_function_arg, hash_function_args, hash_slice,
};
use crate::urc::lock::{
    AdminLockRequest, AdminLockResponse, Lock, LockRequest, LockResponse,
    QueryRequest as LockQueryRequest, QueryResponse as LockQueryResponse, StatusRequest,
    StatusResponse, UnlockRequest, UnlockResponse,
    lock_service_server::{LockService, LockServiceServer},
};

// ============================================================================
// Server Configuration (`default.toml` & CLI `--config`)
// ============================================================================

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TomlFileConfig {
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub environment: EnvironmentSection,
    #[serde(default)]
    pub immutable_store: StoreModeSection,
    #[serde(default)]
    pub mutable_store: MutableStoreSection,
    #[serde(default)]
    pub plugins: PluginsSection,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ServerSection {
    #[serde(default)]
    pub grpc: PortSection,
    #[serde(default)]
    pub quic: PortSection,
    #[serde(default)]
    pub http: PortSection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PortSection {
    #[serde(default = "default_grpc_port")]
    pub port: u16,
}

impl Default for PortSection {
    fn default() -> Self {
        Self {
            port: default_grpc_port(),
        }
    }
}

fn default_grpc_port() -> u16 {
    42337
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct EnvironmentSection {
    #[serde(default)]
    pub endpoint: EndpointSection,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct EndpointSection {
    #[serde(default)]
    pub api_url: String,
    #[serde(default)]
    pub auth_url: String,
    #[serde(default)]
    pub repository_url: String,
    #[serde(default)]
    pub storage_url: String,
    #[serde(default)]
    pub revision_url: String,
    #[serde(default)]
    pub lock_url: String,
    #[serde(default)]
    pub notification_url: String,
    #[serde(default)]
    pub user_url: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct StoreModeSection {
    #[serde(default)]
    pub mode: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MutableStoreSection {
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub local: LocalPathSection,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct LocalPathSection {
    #[serde(default)]
    pub path: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PluginsSection {
    #[serde(default)]
    pub bitlore: Option<BitlorePluginSection>,
    #[serde(default)]
    pub bitlore_peer: Option<BitlorePeerPluginSection>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BitlorePluginSection {
    #[serde(default)]
    pub immutable_store: BitloreStoreConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BitlorePeerPluginSection {
    #[serde(default)]
    pub immutable_store: BitlorePeerStoreConfig,
}

pub fn load_toml_config(config_arg: Option<&str>, default_mode: &str) -> Result<TomlFileConfig> {
    let mut cfg = if let Some(path_str) = config_arg {
        let p = Path::new(path_str);
        let file_path = if p.is_dir() {
            p.join("default.toml")
        } else {
            p.to_path_buf()
        };
        if file_path.exists() {
            let content = std::fs::read_to_string(&file_path)
                .with_context(|| format!("failed to read config file {}", file_path.display()))?;
            toml::from_str::<TomlFileConfig>(&content)
                .with_context(|| format!("failed to parse TOML in {}", file_path.display()))?
        } else {
            TomlFileConfig::default()
        }
    } else {
        TomlFileConfig::default()
    };

    if let Ok(env_mode) = std::env::var("LORE__IMMUTABLE_STORE__MODE")
        && !env_mode.is_empty()
        && cfg.immutable_store.mode.is_empty()
    {
        cfg.immutable_store.mode = env_mode;
    }
    if cfg.immutable_store.mode.is_empty() {
        cfg.immutable_store.mode = default_mode.to_string();
    }
    Ok(cfg)
}

pub async fn run_standalone_server(default_mode: &str) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let args: Vec<String> = std::env::args().collect();
    let mut config_arg: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" && i + 1 < args.len() {
            config_arg = Some(args[i + 1].clone());
            i += 2;
        } else if let Some(rest) = args[i].strip_prefix("--config=") {
            config_arg = Some(rest.to_string());
            i += 1;
        } else {
            i += 1;
        }
    }

    let cfg = load_toml_config(config_arg.as_deref(), default_mode)?;
    let grpc_port = cfg.server.grpc.port;
    let quic_port = if cfg.server.quic.port != 0 {
        cfg.server.quic.port
    } else {
        grpc_port
    };

    let mutable_dir = if !cfg.mutable_store.local.path.is_empty() {
        std::path::PathBuf::from(&cfg.mutable_store.local.path)
    } else {
        std::env::temp_dir()
            .join(format!("bitlore-{default_mode}-{grpc_port}"))
            .join("mutable")
    };
    let mutable_store: Arc<dyn MutableStore> = LocalMutableStore::open(&mutable_dir)?;

    let immutable_store: Arc<dyn ImmutableStore> = match cfg.immutable_store.mode.as_str() {
        PEER_PLUGIN_NAME => {
            let peer_cfg = cfg
                .plugins
                .bitlore_peer
                .map(|p| p.immutable_store)
                .unwrap_or_else(|| BitlorePeerStoreConfig {
                    path: String::new(),
                    tracker_url: "http://127.0.0.1:42340".to_string(),
                    use_n0_relay: false,
                    flush_delay_seconds: 1,
                    max_capacity: None,
                    max_size: None,
                });
            let immut_dir = if !peer_cfg.path.is_empty() {
                std::path::PathBuf::from(&peer_cfg.path)
            } else {
                std::env::temp_dir()
                    .join(format!("bitlore-peer-{grpc_port}"))
                    .join("immutable")
            };
            let local_immut = LocalImmutableStore::open(&immut_dir)?;
            let iroh_node = BitloreIrohNode::bind(local_immut.clone(), peer_cfg.use_n0_relay).await?;
            let tracker_client = BitloreTrackerClient::new(peer_cfg.tracker_url.clone());
            info!(
                path = %immut_dir.display(),
                tracker_url = %peer_cfg.tracker_url,
                peer_id = %iroh_node.peer_id(),
                "Initialized BitlorePeerStore"
            );
            BitlorePeerStore::new_with_persistence_dir(
                local_immut,
                iroh_node,
                tracker_client,
                Some(immut_dir),
            )
        }
        _ => {
            let bitlore_cfg = cfg
                .plugins
                .bitlore
                .map(|p| p.immutable_store)
                .unwrap_or_default();
            let immut_dir = if !bitlore_cfg.path.is_empty() {
                std::path::PathBuf::from(&bitlore_cfg.path)
            } else {
                std::env::temp_dir()
                    .join(format!("bitlore-server-{grpc_port}"))
                    .join("immutable")
            };
            let local_immut = LocalImmutableStore::open(&immut_dir)?;
            let store = Arc::new(BitloreMetadataStore::new(
                local_immut,
                bitlore_cfg.store_file_payloads,
            ));

            let tracker_addr_str = bitlore_cfg.tracker_bind_addr.trim();
            if !tracker_addr_str.is_empty() {
                let bind_addr: SocketAddr = tracker_addr_str
                    .parse()
                    .with_context(|| format!("invalid tracker_bind_addr '{tracker_addr_str}'"))?;
                let ttl = Duration::from_secs(bitlore_cfg.peer_ttl_seconds.max(5));
                let _tracker_state = spawn_tracker_grpc_server(bind_addr, store.clone(), ttl);
                info!(
                    %bind_addr,
                    peer_ttl_seconds = ttl.as_secs(),
                    "Bitlore P2P Tracker gRPC server listening"
                );
            }
            store
        }
    };

    let quic_bind: SocketAddr = format!("0.0.0.0:{quic_port}").parse()?;
    let actual_quic =
        spawn_quic_storage_server(quic_bind, immutable_store.clone(), mutable_store.clone())?;
    info!(%actual_quic, "QUIC storage server listening (lore-storage/0.4 & urc/0.2)");

    let grpc_bind: SocketAddr = format!("0.0.0.0:{grpc_port}").parse()?;
    let shared_state = Arc::new(BitloreServerState {
        endpoint_cfg: cfg.environment.endpoint,
        immutable_store,
        mutable_store,
        locks: RwLock::new(Vec::new()),
    });

    info!(%grpc_bind, mode = %cfg.immutable_store.mode, "Bitlore gRPC server listening");

    tonic::transport::Server::builder()
        .add_service(EnvironmentServiceServer::new(shared_state.clone()))
        .add_service(RepositoryServiceServer::new(shared_state.clone()))
        .add_service(RevisionServiceServer::new(shared_state.clone()))
        .add_service(StorageServiceServer::new(shared_state.clone()))
        .add_service(LockServiceServer::new(shared_state))
        .serve(grpc_bind)
        .await?;

    Ok(())
}

// ============================================================================
// Shared Server State & Metadata Helpers
// ============================================================================

pub struct BitloreServerState {
    pub endpoint_cfg: EndpointSection,
    pub immutable_store: Arc<dyn ImmutableStore>,
    pub mutable_store: Arc<dyn MutableStore>,
    pub locks: RwLock<Vec<(RepositoryId, Lock)>>,
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn parse_repo_id(bytes: &[u8], field_name: &str) -> std::result::Result<RepositoryId, Status> {
    if bytes.len() != 16 {
        return Err(Status::invalid_argument(format!(
            "{field_name} must be 16 bytes, got {}",
            bytes.len()
        )));
    }
    let id = RepositoryId::from(bytes);
    if id.is_zero() {
        return Err(Status::invalid_argument(format!(
            "{field_name} must not be zero"
        )));
    }
    Ok(id)
}

fn parse_branch_id(bytes: &[u8], field_name: &str) -> std::result::Result<BranchId, Status> {
    parse_repo_id(bytes, field_name)
}

fn parse_hash(bytes: &[u8], field_name: &str) -> std::result::Result<Hash, Status> {
    if bytes.len() != 32 {
        return Err(Status::invalid_argument(format!(
            "{field_name} must be 32 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(Hash::from(bytes))
}

fn repo_id_from_metadata_headers<T>(req: &Request<T>) -> std::result::Result<RepositoryId, Status> {
    if let Some(bin_val) = req
        .metadata()
        .get_bin("lore-partition-bin")
        .or_else(|| req.metadata().get_bin("urc-repository-id-bin"))
    {
        let bytes = bin_val
            .to_bytes()
            .map_err(|_| Status::invalid_argument("invalid binary repository header"))?;
        return parse_repo_id(&bytes, "repository header");
    }
    if let Some(val) = req.metadata().get("repository") {
        let s = val
            .to_str()
            .map_err(|_| Status::invalid_argument("invalid 'repository' metadata header"))?;
        return s
            .parse::<RepositoryId>()
            .map_err(|_| Status::invalid_argument("invalid hex in 'repository' metadata header"));
    }
    Err(Status::invalid_argument(
        "missing 'lore-partition-bin' or 'urc-repository-id-bin' metadata header",
    ))
}

fn repo_name_key(name: &str) -> Hash {
    hash_function_arg(SALT_LORE, "id", name)
}

fn repo_metadata_key(repo_id: RepositoryId) -> Hash {
    hash_function_arg(SALT_LORE, "repository-metadata", &repo_id.to_string())
}

fn branch_name_key(name: &str) -> Hash {
    hash_function_arg(SALT_LORE, "id", &name.to_lowercase())
}

fn branch_metadata_key(repo_id: RepositoryId, branch_id: BranchId) -> Hash {
    hash_function_args(
        SALT_LORE,
        "branch-metadata",
        &repo_id.to_string(),
        &branch_id.to_string(),
    )
}

fn branch_head_key(repo_id: RepositoryId, branch_id: BranchId) -> Hash {
    hash_function_args(
        SALT_LORE,
        "branch-head",
        &repo_id.to_string(),
        &branch_id.to_string(),
    )
}

impl BitloreServerState {
    async fn put_metadata_blob(
        &self,
        partition: Partition,
        blob: &MetadataBlob,
    ) -> std::result::Result<Hash, Status> {
        let raw = blob.to_bytes();
        let hash = hash_slice(&raw);
        let address = Address {
            hash,
            context: Context::ZERO,
        };
        let fragment = Fragment {
            flags: FragmentFlags::PayloadStoredDurable.bits(),
            size_payload: raw.len() as u32,
            size_content: raw.len() as u64,
        };
        self.immutable_store
            .clone()
            .put(partition, address, fragment, Some(raw), false)
            .await
            .map_err(|e| Status::internal(format!("failed to store metadata blob: {e}")))?;
        Ok(hash)
    }

    async fn load_metadata_blob(
        &self,
        partition: Partition,
        hash: Hash,
    ) -> std::result::Result<MetadataBlob, Status> {
        let address = Address {
            hash,
            context: Context::ZERO,
        };
        let (fragment, payload) = self
            .immutable_store
            .clone()
            .get(partition, address)
            .await
            .and_then(|d| d.into_payload())
            .map_err(|_| Status::not_found(format!("metadata blob {hash} not found")))?;
        let decompressed = crate::types::decompress_fragment(fragment, &payload)
            .map_err(|e| Status::internal(format!("failed to decompress metadata blob: {e}")))?;
        MetadataBlob::from_bytes(&decompressed)
            .map_err(|e| Status::internal(format!("failed to decode metadata blob: {e}")))
    }

    async fn load_repository_proto(
        &self,
        repo_id: RepositoryId,
    ) -> std::result::Result<ProtoRepository, Status> {
        let meta_hash = self
            .mutable_store
            .clone()
            .load(
                repo_id,
                repo_metadata_key(repo_id),
                KeyType::RepositoryMetadata,
            )
            .await
            .map_err(|_| Status::not_found(format!("repository {repo_id} not found")))?;
        let blob = self.load_metadata_blob(repo_id, meta_hash).await?;
        let name = blob.get_string("name").unwrap_or_default();
        let description = blob.get_string("description").unwrap_or_default();
        let default_branch_id = blob.get_context("default-branch").unwrap_or(Context::ZERO);
        let default_branch_name = blob.get_string("default-branch-name").unwrap_or_default();
        let creator = blob.get_string("creator").unwrap_or_default();
        let created = blob.get_numeric("created").unwrap_or(0);

        Ok(ProtoRepository {
            id: Bytes::copy_from_slice(repo_id.as_bytes()),
            name,
            description,
            default_branch_id: Bytes::copy_from_slice(default_branch_id.as_bytes()),
            default_branch_name,
            creator,
            created,
            metadata: Bytes::copy_from_slice(meta_hash.as_bytes()),
        })
    }

    async fn load_branch_proto(
        &self,
        repo_id: RepositoryId,
        branch_id: BranchId,
    ) -> std::result::Result<ProtoBranch, Status> {
        let meta_hash = self
            .mutable_store
            .clone()
            .load(
                repo_id,
                branch_metadata_key(repo_id, branch_id),
                KeyType::BranchMetadata,
            )
            .await
            .map_err(|_| Status::not_found(format!("branch {branch_id} not found")))?;

        let blob = self.load_metadata_blob(repo_id, meta_hash).await?;
        let name = blob.get_string("name").unwrap_or_default();
        let creator = blob.get_string("creator").unwrap_or_default();
        let category = blob.get_string("category").unwrap_or_default();
        let created = blob.get_numeric("created").unwrap_or(0);

        let mut stack = Vec::new();
        if let Some(stack_bytes) = blob.get_binary("stack") {
            for chunk in stack_bytes.chunks_exact(48) {
                stack.push(ProtoBranchPoint {
                    branch_id: Bytes::copy_from_slice(&chunk[0..16]),
                    revision_signature: Bytes::copy_from_slice(&chunk[16..48]),
                });
            }
        }

        let latest = self
            .mutable_store
            .clone()
            .load(
                repo_id,
                branch_head_key(repo_id, branch_id),
                KeyType::BranchLatestPointer,
            )
            .await
            .unwrap_or(Hash::ZERO);

        let mapped_id = if !name.is_empty() {
            self.mutable_store
                .clone()
                .load(repo_id, branch_name_key(&name), KeyType::BranchId)
                .await
                .map(|h| h.to_context())
                .unwrap_or(Context::ZERO)
        } else {
            Context::ZERO
        };
        let deleted = mapped_id != branch_id;

        Ok(ProtoBranch {
            id: Bytes::copy_from_slice(branch_id.as_bytes()),
            name,
            creator,
            category,
            created,
            latest: Bytes::copy_from_slice(latest.as_bytes()),
            deleted,
            metadata: Bytes::copy_from_slice(meta_hash.as_bytes()),
            stack,
        })
    }

    async fn load_state_view(
        &self,
        repo_id: RepositoryId,
        signature: Hash,
    ) -> std::result::Result<StateDataView, Status> {
        let address = Address {
            hash: signature,
            context: Context::ZERO,
        };
        let (fragment, payload) = self
            .immutable_store
            .clone()
            .get(repo_id, address)
            .await
            .and_then(|d| d.into_payload())
            .map_err(|_| Status::not_found(format!("revision state {signature} not found")))?;
        StateDataView::parse(fragment, &payload)
            .map_err(|e| Status::internal(format!("failed to parse StateData {signature}: {e}")))
    }

    async fn resolve_revision_number_on_branch(
        &self,
        repo_id: RepositoryId,
        branch_id: BranchId,
        target_number: u64,
    ) -> std::result::Result<(Hash, StateDataView), Status> {
        // Ensure branch exists
        let _branch = self.load_branch_proto(repo_id, branch_id).await?;
        let tip = self
            .mutable_store
            .clone()
            .load(
                repo_id,
                branch_head_key(repo_id, branch_id),
                KeyType::BranchLatestPointer,
            )
            .await
            .unwrap_or(Hash::ZERO);

        if tip.is_zero() {
            return Err(Status::not_found(format!(
                "branch {branch_id} has no revisions"
            )));
        }

        let mut cur_sig = tip;
        loop {
            let view = self.load_state_view(repo_id, cur_sig).await?;
            if target_number == 0 || view.revision_number == target_number {
                return Ok((cur_sig, view));
            }
            if view.revision_number < target_number || view.parent_self.is_zero() {
                break;
            }
            cur_sig = view.parent_self;
        }

        Err(Status::not_found(format!(
            "revision #{target_number} not found on branch {branch_id}"
        )))
    }
}

// ============================================================================
// 1. EnvironmentService
// ============================================================================

#[async_trait::async_trait]
impl EnvironmentService for Arc<BitloreServerState> {
    async fn environment_get(
        &self,
        _request: Request<EnvironmentGetRequest>,
    ) -> std::result::Result<Response<EnvironmentGetResponse>, Status> {
        let ep = &self.endpoint_cfg;
        let pick = |specific: &str| -> String {
            if !specific.is_empty() {
                specific.to_string()
            } else {
                ep.api_url.clone()
            }
        };
        let endpoint = ProtoEndpoint {
            auth_url: ep.auth_url.clone(),
            repository_url: pick(&ep.repository_url),
            storage_url: pick(&ep.storage_url),
            revision_url: pick(&ep.revision_url),
            lock_url: pick(&ep.lock_url),
            notification_url: pick(&ep.notification_url),
            user_url: ep.user_url.clone(),
        };
        Ok(Response::new(EnvironmentGetResponse {
            environment: Some(ProtoEnvironment {
                endpoint: Some(endpoint),
                config: Some(ProtoEnvConfig {
                    max_query_batch: 0,
                    compression_mode: None,
                }),
                oidc: None,
            }),
        }))
    }
}

// ============================================================================
// 2. RepositoryService
// ============================================================================

type BoxStream<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

#[async_trait::async_trait]
impl RepositoryService for Arc<BitloreServerState> {
    type RepositoryListStream = BoxStream<RepositoryListResponse>;

    async fn repository_create(
        &self,
        request: Request<RepositoryCreateRequest>,
    ) -> std::result::Result<Response<RepositoryCreateResponse>, Status> {
        let req = request.into_inner();
        let repo_id = parse_repo_id(&req.id, "id")?;
        let default_branch_id = parse_branch_id(&req.default_branch_id, "default_branch_id")?;
        if req.name.is_empty() || req.default_branch_name.is_empty() {
            return Err(Status::invalid_argument(
                "name and default_branch_name must not be empty",
            ));
        }

        let name_key = repo_name_key(&req.name);
        let existing_id = self
            .mutable_store
            .clone()
            .compare_and_swap(
                Partition::ZERO,
                name_key,
                Hash::ZERO,
                Hash::from_context(repo_id),
                KeyType::RepositoryId,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if !existing_id.is_zero() && existing_id != Hash::from_context(repo_id) {
            return Err(Status::already_exists(format!(
                "repository '{}' already exists",
                req.name
            )));
        }

        let created_ms = now_unix_ms();
        let creator = req.creator.unwrap_or_default();

        // 1. Write default branch metadata & name mapping
        let mut branch_meta = MetadataBlob::new();
        branch_meta.set_binary("id", default_branch_id.as_bytes());
        branch_meta.set_string("name", &req.default_branch_name);
        branch_meta.set_string("category", "");
        branch_meta.set_string("creator", &creator);
        branch_meta.set_numeric("created", created_ms);
        branch_meta.set_boolean("protect", false);
        let branch_meta_hash = self.put_metadata_blob(repo_id, &branch_meta).await?;

        self.mutable_store
            .clone()
            .store(
                repo_id,
                branch_metadata_key(repo_id, default_branch_id),
                branch_meta_hash,
                KeyType::BranchMetadata,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.mutable_store
            .clone()
            .store(
                repo_id,
                branch_name_key(&req.default_branch_name),
                Hash::from_context(default_branch_id),
                KeyType::BranchId,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // 2. Write repository metadata
        let mut repo_meta = MetadataBlob::new();
        repo_meta.set_string("name", &req.name);
        repo_meta.set_string("description", &req.description);
        repo_meta.set_context("default-branch", default_branch_id);
        repo_meta.set_string("default-branch-name", &req.default_branch_name);
        repo_meta.set_string("creator", &creator);
        repo_meta.set_numeric("created", created_ms);
        let repo_meta_hash = self.put_metadata_blob(repo_id, &repo_meta).await?;

        self.mutable_store
            .clone()
            .store(
                repo_id,
                repo_metadata_key(repo_id),
                repo_meta_hash,
                KeyType::RepositoryMetadata,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RepositoryCreateResponse {
            repository: Some(ProtoRepository {
                id: Bytes::copy_from_slice(repo_id.as_bytes()),
                name: req.name,
                description: req.description,
                default_branch_id: Bytes::copy_from_slice(default_branch_id.as_bytes()),
                default_branch_name: req.default_branch_name,
                creator,
                created: created_ms,
                metadata: Bytes::copy_from_slice(repo_meta_hash.as_bytes()),
            }),
        }))
    }

    async fn repository_get(
        &self,
        request: Request<RepositoryGetRequest>,
    ) -> std::result::Result<Response<RepositoryGetResponse>, Status> {
        let req = request.into_inner();
        let repo_id = match req.query {
            Some(RepoGetQuery::Id(id_bytes)) => parse_repo_id(&id_bytes, "id")?,
            Some(RepoGetQuery::Name(name)) => {
                let id_hash = self
                    .mutable_store
                    .clone()
                    .load(Partition::ZERO, repo_name_key(&name), KeyType::RepositoryId)
                    .await
                    .map_err(|_| Status::not_found(format!("repository '{name}' not found")))?;
                id_hash.to_context()
            }
            None => return Err(Status::invalid_argument("missing query")),
        };

        let repository = self.load_repository_proto(repo_id).await?;
        Ok(Response::new(RepositoryGetResponse {
            repository: Some(repository),
        }))
    }

    async fn repository_list(
        &self,
        request: Request<RepositoryListRequest>,
    ) -> std::result::Result<Response<Self::RepositoryListStream>, Status> {
        let req = request.into_inner();
        let mut stream = self
            .mutable_store
            .clone()
            .list(Partition::ZERO, KeyType::RepositoryId)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut repositories = Vec::new();
        while let Some(Ok((_key, val))) = stream.next().await {
            let repo_id = val.to_context();
            if let Ok(repo) = self.load_repository_proto(repo_id).await {
                if let Some(ref creator_filter) = req.creator
                    && &repo.creator != creator_filter
                {
                    continue;
                }
                repositories.push(repo);
            }
        }
        repositories.sort_by(|a, b| a.name.cmp(&b.name));

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        crate::spawn_bg(async move {
            for repo in repositories {
                if tx
                    .send(Ok(RepositoryListResponse {
                        repository: Some(repo),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn repository_delete(
        &self,
        request: Request<RepositoryDeleteRequest>,
    ) -> std::result::Result<Response<RepositoryDeleteResponse>, Status> {
        let req = request.into_inner();
        let repo_id = parse_repo_id(&req.id, "id")?;
        let repo = self.load_repository_proto(repo_id).await?;

        let _ = self
            .mutable_store
            .clone()
            .store(
                Partition::ZERO,
                repo_name_key(&repo.name),
                Hash::ZERO,
                KeyType::RepositoryId,
            )
            .await;
        let _ = self
            .mutable_store
            .clone()
            .store(
                repo_id,
                repo_metadata_key(repo_id),
                Hash::ZERO,
                KeyType::RepositoryMetadata,
            )
            .await;

        Ok(Response::new(RepositoryDeleteResponse {
            repository: Some(repo),
        }))
    }

    async fn repository_metadata_get(
        &self,
        request: Request<RepositoryMetadataGetRequest>,
    ) -> std::result::Result<Response<RepositoryMetadataGetResponse>, Status> {
        let req = request.into_inner();
        let repo_id = parse_repo_id(&req.id, "id")?;
        let meta_hash = self
            .mutable_store
            .clone()
            .load(
                repo_id,
                repo_metadata_key(repo_id),
                KeyType::RepositoryMetadata,
            )
            .await
            .map_err(|_| Status::not_found(format!("repository {repo_id} not found")))?;
        Ok(Response::new(RepositoryMetadataGetResponse {
            metadata: Bytes::copy_from_slice(meta_hash.as_bytes()),
        }))
    }

    async fn repository_metadata_set(
        &self,
        request: Request<RepositoryMetadataSetRequest>,
    ) -> std::result::Result<Response<RepositoryMetadataSetResponse>, Status> {
        let req = request.into_inner();
        let repo_id = parse_repo_id(&req.id, "id")?;
        let expected = parse_hash(&req.expected, "expected")?;
        let updated = parse_hash(&req.updated, "updated")?;

        let prev = self
            .mutable_store
            .clone()
            .compare_and_swap(
                repo_id,
                repo_metadata_key(repo_id),
                expected,
                updated,
                KeyType::RepositoryMetadata,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let current_meta = if prev == expected { updated } else { prev };
        Ok(Response::new(RepositoryMetadataSetResponse {
            metadata: Bytes::copy_from_slice(current_meta.as_bytes()),
        }))
    }
}

// ============================================================================
// 3. RevisionService
// ============================================================================

#[async_trait::async_trait]
impl RevisionService for Arc<BitloreServerState> {
    type BranchListStream = BoxStream<BranchListResponse>;

    async fn branch_create(
        &self,
        request: Request<BranchCreateRequest>,
    ) -> std::result::Result<Response<BranchCreateResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let branch_id = parse_branch_id(&req.id, "id")?;
        if req.name.is_empty() {
            return Err(Status::invalid_argument("branch name must not be empty"));
        }

        let name_key = branch_name_key(&req.name);
        let existing = self
            .mutable_store
            .clone()
            .compare_and_swap(
                repo_id,
                name_key,
                Hash::ZERO,
                Hash::from_context(branch_id),
                KeyType::BranchId,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if !existing.is_zero() && existing != Hash::from_context(branch_id) {
            return Err(Status::already_exists(format!(
                "branch '{}' already exists",
                req.name
            )));
        }

        let created_ms = now_unix_ms();
        let creator = req.creator.unwrap_or_default();
        let mut stack_bytes = Vec::with_capacity(req.stack.len() * 48);
        for bp in &req.stack {
            let b_id = parse_branch_id(&bp.branch_id, "stack.branch_id")?;
            let rev_sig = parse_hash(&bp.revision_signature, "stack.revision_signature")?;
            stack_bytes.extend_from_slice(b_id.as_bytes());
            stack_bytes.extend_from_slice(rev_sig.as_bytes());
        }

        let mut branch_meta = MetadataBlob::new();
        branch_meta.set_binary("id", branch_id.as_bytes());
        branch_meta.set_string("name", &req.name);
        branch_meta.set_string("category", &req.category);
        branch_meta.set_string("creator", &creator);
        branch_meta.set_numeric("created", created_ms);
        branch_meta.set_boolean("protect", false);
        if !stack_bytes.is_empty() {
            branch_meta.set_binary("stack", &stack_bytes);
            let first_branch = BranchId::from(&stack_bytes[0..16]);
            let first_rev = Hash::from(&stack_bytes[16..48]);
            branch_meta.set_context("parent", first_branch);
            branch_meta.set_hash("branch-point", first_rev);
        }

        let meta_hash = self.put_metadata_blob(repo_id, &branch_meta).await?;
        self.mutable_store
            .clone()
            .store(
                repo_id,
                branch_metadata_key(repo_id, branch_id),
                meta_hash,
                KeyType::BranchMetadata,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let latest = if let Some(first) = req.stack.first() {
            parse_hash(&first.revision_signature, "stack[0].revision_signature")?
        } else {
            Hash::ZERO
        };

        if !latest.is_zero() {
            self.mutable_store
                .clone()
                .store(
                    repo_id,
                    branch_head_key(repo_id, branch_id),
                    latest,
                    KeyType::BranchLatestPointer,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        let branch = self.load_branch_proto(repo_id, branch_id).await?;
        Ok(Response::new(BranchCreateResponse {
            branch: Some(branch),
        }))
    }

    async fn branch_get(
        &self,
        request: Request<BranchGetRequest>,
    ) -> std::result::Result<Response<BranchGetResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let branch_id = match req.query {
            Some(BranchGetQuery::Id(id_bytes)) => parse_branch_id(&id_bytes, "id")?,
            Some(BranchGetQuery::Name(name)) => {
                let h = self
                    .mutable_store
                    .clone()
                    .load(repo_id, branch_name_key(&name), KeyType::BranchId)
                    .await
                    .map_err(|_| Status::not_found(format!("branch '{name}' not found")))?;
                h.to_context()
            }
            None => return Err(Status::invalid_argument("missing query")),
        };

        let branch = self.load_branch_proto(repo_id, branch_id).await?;
        Ok(Response::new(BranchGetResponse {
            branch: Some(branch),
        }))
    }

    async fn branch_list(
        &self,
        request: Request<BranchListRequest>,
    ) -> std::result::Result<Response<Self::BranchListStream>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let _repo = self.load_repository_proto(repo_id).await?;

        let mut stream = self
            .mutable_store
            .clone()
            .list(repo_id, KeyType::BranchId)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut branches = Vec::new();
        while let Some(Ok((_key, val))) = stream.next().await {
            let branch_id = val.to_context();
            if branch_id.is_zero() {
                continue;
            }
            if let Ok(branch) = self.load_branch_proto(repo_id, branch_id).await {
                if branch.deleted && !req.include_deleted {
                    continue;
                }
                if let Some(ref creator_filter) = req.creator
                    && &branch.creator != creator_filter
                {
                    continue;
                }
                branches.push(branch);
            }
        }
        branches.sort_by(|a, b| a.name.cmp(&b.name));

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        crate::spawn_bg(async move {
            for branch in branches {
                if tx
                    .send(Ok(BranchListResponse {
                        branch: Some(branch),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn branch_delete(
        &self,
        request: Request<BranchDeleteRequest>,
    ) -> std::result::Result<Response<BranchDeleteResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let branch_id = parse_branch_id(&req.id, "id")?;

        let current = self.load_branch_proto(repo_id, branch_id).await?;
        let _ = self
            .mutable_store
            .clone()
            .store(
                repo_id,
                branch_name_key(&current.name),
                Hash::ZERO,
                KeyType::BranchId,
            )
            .await;

        let updated = self.load_branch_proto(repo_id, branch_id).await?;
        Ok(Response::new(BranchDeleteResponse {
            branch: Some(updated),
        }))
    }

    async fn branch_metadata_get(
        &self,
        request: Request<BranchMetadataGetRequest>,
    ) -> std::result::Result<Response<BranchMetadataGetResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let branch_id = parse_branch_id(&req.id, "id")?;
        let meta_hash = self
            .mutable_store
            .clone()
            .load(
                repo_id,
                branch_metadata_key(repo_id, branch_id),
                KeyType::BranchMetadata,
            )
            .await
            .map_err(|_| Status::not_found(format!("branch {branch_id} not found")))?;
        Ok(Response::new(BranchMetadataGetResponse {
            metadata: Bytes::copy_from_slice(meta_hash.as_bytes()),
        }))
    }

    async fn branch_metadata_set(
        &self,
        request: Request<BranchMetadataSetRequest>,
    ) -> std::result::Result<Response<BranchMetadataSetResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let branch_id = parse_branch_id(&req.id, "id")?;
        let expected = parse_hash(&req.expected, "expected")?;
        let updated = parse_hash(&req.updated, "updated")?;

        let prev = self
            .mutable_store
            .clone()
            .compare_and_swap(
                repo_id,
                branch_metadata_key(repo_id, branch_id),
                expected,
                updated,
                KeyType::BranchMetadata,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let current_meta = if prev == expected { updated } else { prev };
        Ok(Response::new(BranchMetadataSetResponse {
            metadata: Bytes::copy_from_slice(current_meta.as_bytes()),
        }))
    }

    async fn branch_push(
        &self,
        request: Request<BranchPushRequest>,
    ) -> std::result::Result<Response<BranchPushResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let branch_id = parse_branch_id(&req.id, "id")?;
        let signature = parse_hash(&req.revision_signature, "revision_signature")?;

        // Verify the branch exists and the pushed revision's StateData is present in `immutable_store`
        let _branch = self.load_branch_proto(repo_id, branch_id).await?;
        let state_view = self.load_state_view(repo_id, signature).await?;

        let head_key = branch_head_key(repo_id, branch_id);
        let current_tip = self
            .mutable_store
            .clone()
            .load(repo_id, head_key, KeyType::BranchLatestPointer)
            .await
            .unwrap_or(Hash::ZERO);

        if !req.force && !current_tip.is_zero() && current_tip != signature {
            let mut is_ancestor = false;
            let mut queue = std::collections::VecDeque::from([signature]);
            let mut visited = HashSet::new();
            while let Some(sig) = queue.pop_front() {
                if sig.is_zero() || !visited.insert(sig) || visited.len() > 500 {
                    continue;
                }
                if sig == current_tip {
                    is_ancestor = true;
                    break;
                }
                if let Ok(view) = self.load_state_view(repo_id, sig).await {
                    if !view.parent_self.is_zero() {
                        queue.push_back(view.parent_self);
                    }
                    if !view.parent_other.is_zero() {
                        queue.push_back(view.parent_other);
                    }
                }
            }
            if !is_ancestor {
                return Err(Status::failed_precondition(format!(
                    "branch tip {current_tip} is not an ancestor of pushed revision {signature}"
                )));
            }
        }

        let prev = self
            .mutable_store
            .clone()
            .compare_and_swap(
                repo_id,
                head_key,
                current_tip,
                signature,
                KeyType::BranchLatestPointer,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if prev != current_tip {
            return Err(Status::failed_precondition(format!(
                "branch tip advanced concurrently: expected {current_tip}, actual {prev}"
            )));
        }

        Ok(Response::new(BranchPushResponse {
            revision_signature: Bytes::copy_from_slice(signature.as_bytes()),
            revision_number: state_view.revision_number,
            fast_forward_merged: false,
            message: None,
        }))
    }

    async fn revision_list(
        &self,
        request: Request<RevisionListRequest>,
    ) -> std::result::Result<Response<RevisionListResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let count = 100usize;

        let start_sig_opt = match req.start {
            Some(RevListStart::Identifier(id)) => {
                let b_id = parse_branch_id(&id.branch_id, "start.identifier.branch_id")?;
                let _branch = self.load_branch_proto(repo_id, b_id).await?;
                let tip = self
                    .mutable_store
                    .clone()
                    .load(
                        repo_id,
                        branch_head_key(repo_id, b_id),
                        KeyType::BranchLatestPointer,
                    )
                    .await
                    .unwrap_or(Hash::ZERO);
                if tip.is_zero() {
                    if id.number == 0 {
                        None
                    } else {
                        return Err(Status::not_found("revision not found on empty branch"));
                    }
                } else {
                    let (sig, _) = self
                        .resolve_revision_number_on_branch(repo_id, b_id, id.number)
                        .await?;
                    Some(sig)
                }
            }
            Some(RevListStart::Signature(sig_bytes)) => {
                let sig = parse_hash(&sig_bytes, "start.signature")?;
                let _ = self.load_state_view(repo_id, sig).await?;
                Some(sig)
            }
            None => return Err(Status::invalid_argument("missing start")),
        };

        let mut items = Vec::new();
        let mut cur_opt = start_sig_opt;
        while let Some(cur_sig) = cur_opt {
            if cur_sig.is_zero() || items.len() >= count {
                break;
            }
            let Ok(view) = self.load_state_view(repo_id, cur_sig).await else {
                cur_opt = None;
                break;
            };
            let parent = view.parent_self;
            items.push(ProtoRevisionItem {
                number: view.revision_number,
                signature: Bytes::copy_from_slice(cur_sig.as_bytes()),
                metadata: Bytes::copy_from_slice(view.hash_metadata.as_bytes()),
                state: view.raw_bytes,
            });
            cur_opt = if parent.is_zero() { None } else { Some(parent) };
        }

        let signature_backward = cur_opt
            .filter(|h| !h.is_zero())
            .map(|h| Bytes::copy_from_slice(h.as_bytes()));

        Ok(Response::new(RevisionListResponse {
            items,
            signature_forward: None,
            signature_backward,
        }))
    }
}

// ============================================================================
// 4. StorageService (gRPC fallback for storage RPCs)
// ============================================================================

fn item_ok() -> Option<ItemStatus> {
    Some(ItemStatus {
        code: 0,
        message: String::new(),
    })
}

fn item_err(code: tonic::Code, msg: impl Into<String>) -> Option<ItemStatus> {
    Some(ItemStatus {
        code: code as u32,
        message: msg.into(),
    })
}

#[async_trait::async_trait]
impl StorageService for Arc<BitloreServerState> {
    type GetStream = BoxStream<GetResponse>;
    type GetMetadataStream = BoxStream<GetResponse>;
    type GetResolvedStream = BoxStream<GetResolvedResponse>;
    type PutResolvedStream = BoxStream<PutResolvedResponse>;
    type PutStream = BoxStream<PutResponse>;
    type CopyStream = BoxStream<CopyResponse>;

    async fn get(
        &self,
        request: Request<Streaming<ProtoAddress>>,
    ) -> std::result::Result<Response<Self::GetStream>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let mut inbound = request.into_inner();
        let state = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        crate::spawn_bg(async move {
            while let Some(Ok(addr_proto)) = inbound.next().await {
                let addr = Address {
                    hash: Hash::from(addr_proto.hash.as_ref()),
                    context: Context::from(addr_proto.context.as_ref()),
                };
                match state
                    .immutable_store
                    .clone()
                    .get(repo_id, addr)
                    .await
                    .and_then(|d| d.into_payload())
                {
                    Ok((mut frag, payload)) => {
                        frag.flags &= !FragmentFlags::PayloadStored.bits();
                        frag.flags |= FragmentFlags::PayloadStoredDurable.bits();
                        let _ = tx
                            .send(Ok(GetResponse {
                                address: Some(addr_proto),
                                fragment: Some(ProtoFragment {
                                    flags: frag.flags,
                                    size_payload: frag.size_payload,
                                    size_content: frag.size_content,
                                }),
                                payload,
                                status: item_ok(),
                            }))
                            .await;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Ok(GetResponse {
                                address: Some(addr_proto),
                                fragment: None,
                                payload: Bytes::new(),
                                status: item_err(tonic::Code::NotFound, e.to_string()),
                            }))
                            .await;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_metadata(
        &self,
        request: Request<Streaming<ProtoAddress>>,
    ) -> std::result::Result<Response<Self::GetMetadataStream>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let mut inbound = request.into_inner();
        let state = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        crate::spawn_bg(async move {
            while let Some(Ok(addr_proto)) = inbound.next().await {
                let addr = Address {
                    hash: Hash::from(addr_proto.hash.as_ref()),
                    context: Context::from(addr_proto.context.as_ref()),
                };
                match state.immutable_store.clone().get_metadata(repo_id, addr).await {
                    Ok(data) if data.match_made != StoreMatch::MatchNone => {
                        let mut frag = data.fragment;
                        frag.flags &= !FragmentFlags::PayloadStored.bits();
                        frag.flags |= FragmentFlags::PayloadStoredDurable.bits();
                        let _ = tx
                            .send(Ok(GetResponse {
                                address: Some(addr_proto),
                                fragment: Some(ProtoFragment {
                                    flags: frag.flags,
                                    size_payload: frag.size_payload,
                                    size_content: frag.size_content,
                                }),
                                payload: Bytes::new(),
                                status: item_ok(),
                            }))
                            .await;
                    }
                    _ => {
                        let _ = tx
                            .send(Ok(GetResponse {
                                address: Some(addr_proto),
                                fragment: None,
                                payload: Bytes::new(),
                                status: item_err(tonic::Code::NotFound, "not found"),
                            }))
                            .await;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_resolved(
        &self,
        request: Request<Streaming<GetResolvedRequest>>,
    ) -> std::result::Result<Response<Self::GetResolvedStream>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let mut inbound = request.into_inner();
        let state = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        crate::spawn_bg(async move {
            while let Some(Ok(req)) = inbound.next().await {
                let req_id = req.request_id;
                let Some(key_proto) = req.key else {
                    let _ = tx
                        .send(Ok(GetResolvedResponse {
                            request_id: req_id,
                            status: item_err(tonic::Code::InvalidArgument, "missing key"),
                            resolved: Bytes::new(),
                            fragment: None,
                            payload: Bytes::new(),
                        }))
                        .await;
                    continue;
                };
                let key = Hash::from(key_proto.hash.as_ref());
                let context = Context::from(key_proto.context.as_ref());
                let resolved = match state
                    .mutable_store
                    .clone()
                    .load(repo_id, key, KeyType::Resolve)
                    .await
                {
                    Ok(h) => h,
                    Err(e) => {
                        let _ = tx
                            .send(Ok(GetResolvedResponse {
                                request_id: req_id,
                                status: item_err(tonic::Code::NotFound, e.to_string()),
                                resolved: Bytes::new(),
                                fragment: None,
                                payload: Bytes::new(),
                            }))
                            .await;
                        continue;
                    }
                };
                match state
                    .immutable_store
                    .clone()
                    .get(
                        repo_id,
                        Address {
                            hash: resolved,
                            context,
                        },
                    )
                    .await
                    .and_then(|d| d.into_payload())
                {
                    Ok((mut frag, payload)) => {
                        frag.flags &= !FragmentFlags::PayloadStored.bits();
                        frag.flags |= FragmentFlags::PayloadStoredDurable.bits();
                        let _ = tx
                            .send(Ok(GetResolvedResponse {
                                request_id: req_id,
                                status: item_ok(),
                                resolved: Bytes::copy_from_slice(resolved.as_bytes()),
                                fragment: Some(ProtoFragment {
                                    flags: frag.flags,
                                    size_payload: frag.size_payload,
                                    size_content: frag.size_content,
                                }),
                                payload,
                            }))
                            .await;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Ok(GetResolvedResponse {
                                request_id: req_id,
                                status: item_err(tonic::Code::NotFound, e.to_string()),
                                resolved: Bytes::new(),
                                fragment: None,
                                payload: Bytes::new(),
                            }))
                            .await;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn put_resolved(
        &self,
        request: Request<Streaming<PutResolvedRequest>>,
    ) -> std::result::Result<Response<Self::PutResolvedStream>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let mut inbound = request.into_inner();
        let state = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        crate::spawn_bg(async move {
            while let Some(Ok(req)) = inbound.next().await {
                let req_id = req.request_id;
                let key = Hash::from(req.key.as_ref());
                let Some(addr_proto) = req.address else {
                    let _ = tx
                        .send(Ok(PutResolvedResponse {
                            request_id: req_id,
                            status: item_err(tonic::Code::InvalidArgument, "missing address"),
                        }))
                        .await;
                    continue;
                };
                let addr = Address {
                    hash: Hash::from(addr_proto.hash.as_ref()),
                    context: Context::from(addr_proto.context.as_ref()),
                };
                if !addr.hash.is_zero()
                    && let Some(frag_proto) = req.fragment
                {
                    let frag = Fragment {
                        flags: frag_proto.flags & !FragmentFlags::PayloadStored.bits(),
                        size_payload: frag_proto.size_payload,
                        size_content: frag_proto.size_content,
                    };
                    let _ = state
                        .immutable_store
                        .clone()
                        .put(repo_id, addr, frag, Some(req.payload), false)
                        .await;
                }
                let _ = state
                    .mutable_store
                    .clone()
                    .store(repo_id, key, addr.hash, KeyType::Resolve)
                    .await;
                let _ = tx
                    .send(Ok(PutResolvedResponse {
                        request_id: req_id,
                        status: item_ok(),
                    }))
                    .await;
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn put(
        &self,
        request: Request<Streaming<PutRequest>>,
    ) -> std::result::Result<Response<Self::PutStream>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let mut inbound = request.into_inner();
        let state = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        crate::spawn_bg(async move {
            while let Some(Ok(req)) = inbound.next().await {
                let (Some(addr_proto), Some(frag_proto)) = (req.address.clone(), req.fragment)
                else {
                    let _ = tx
                        .send(Ok(PutResponse {
                            address: req.address,
                            status: item_err(tonic::Code::InvalidArgument, "missing fields"),
                        }))
                        .await;
                    continue;
                };
                let addr = Address {
                    hash: Hash::from(addr_proto.hash.as_ref()),
                    context: Context::from(addr_proto.context.as_ref()),
                };
                let frag = Fragment {
                    flags: frag_proto.flags & !FragmentFlags::PayloadStored.bits(),
                    size_payload: frag_proto.size_payload,
                    size_content: frag_proto.size_content,
                };
                let data_opt = req.payload.filter(|p| !p.is_empty());
                match state
                    .immutable_store
                    .clone()
                    .put(repo_id, addr, frag, data_opt, false)
                    .await
                {
                    Ok(_) => {
                        let _ = tx
                            .send(Ok(PutResponse {
                                address: Some(addr_proto),
                                status: item_ok(),
                            }))
                            .await;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Ok(PutResponse {
                                address: Some(addr_proto),
                                status: item_err(tonic::Code::Internal, e.to_string()),
                            }))
                            .await;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn query(
        &self,
        request: Request<StorageQueryRequest>,
    ) -> std::result::Result<Response<StorageQueryResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let addresses: Vec<Address> = req
            .addresses
            .iter()
            .map(|a| Address {
                hash: Hash::from(a.hash.as_ref()),
                context: Context::from(a.context.as_ref()),
            })
            .collect();
        let mut resolved = vec![Default::default(); addresses.len()];
        self.immutable_store
            .clone()
            .query(repo_id, &addresses, &mut resolved)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let results: Vec<i32> = resolved
            .iter()
            .map(|r| match r.match_made {
                StoreMatch::MatchFull => 0,
                StoreMatch::MatchPartition => 1,
                StoreMatch::MatchNone | StoreMatch::MatchHash => 3,
            })
            .collect();
        Ok(Response::new(StorageQueryResponse { results }))
    }

    async fn verify(
        &self,
        request: Request<VerifyRequest>,
    ) -> std::result::Result<Response<VerifyResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let Some(addr_proto) = req.address else {
            return Err(Status::invalid_argument("missing address"));
        };
        let addr = Address {
            hash: Hash::from(addr_proto.hash.as_ref()),
            context: Context::from(addr_proto.context.as_ref()),
        };
        self.immutable_store
            .clone()
            .get(repo_id, addr)
            .await
            .map_err(|_| Status::not_found("fragment not found"))?;
        Ok(Response::new(VerifyResponse {
            healed: 0,
            corrupted: false,
        }))
    }

    async fn copy(
        &self,
        request: Request<Streaming<CopyRequest>>,
    ) -> std::result::Result<Response<Self::CopyStream>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let mut inbound = request.into_inner();
        let state = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        crate::spawn_bg(async move {
            while let Some(Ok(req)) = inbound.next().await {
                let src_repo_bytes = req.source_repository_id.clone();
                let src_repo = Partition::from(src_repo_bytes.as_ref());
                let Some(src_addr_proto) = req.source_address else {
                    continue;
                };
                let src_addr = Address {
                    hash: Hash::from(src_addr_proto.hash.as_ref()),
                    context: Context::from(src_addr_proto.context.as_ref()),
                };
                let target_ctx = if req.target_context.len() == 16 {
                    let ctx = Context::from(req.target_context.as_ref());
                    if ctx.is_zero() {
                        src_addr.context
                    } else {
                        ctx
                    }
                } else {
                    src_addr.context
                };
                match state
                    .immutable_store
                    .clone()
                    .copy(
                        src_repo,
                        src_addr,
                        repo_id,
                        target_ctx,
                        CopyBehavior {
                            durable: true,
                            do_not_replicate: false,
                        },
                    )
                    .await
                {
                    Ok(()) => {
                        let _ = tx
                            .send(Ok(CopyResponse {
                                source_repository_id: src_repo_bytes,
                                source_address: Some(src_addr_proto),
                                status: item_ok(),
                            }))
                            .await;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Ok(CopyResponse {
                                source_repository_id: src_repo_bytes,
                                source_address: Some(src_addr_proto),
                                status: item_err(tonic::Code::NotFound, e.to_string()),
                            }))
                            .await;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn mutable_load(
        &self,
        request: Request<MutableLoadRequest>,
    ) -> std::result::Result<Response<MutableLoadResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let key = parse_hash(&req.key, "key")?;
        let key_type = KeyType::try_from(req.key_type as u8)
            .map_err(|_| Status::invalid_argument("invalid key_type"))?;
        let val = self
            .mutable_store
            .clone()
            .load(repo_id, key, key_type)
            .await
            .map_err(|_| Status::not_found("mutable key not found"))?;
        Ok(Response::new(MutableLoadResponse {
            value: Bytes::copy_from_slice(val.as_bytes()),
        }))
    }

    async fn mutable_store(
        &self,
        request: Request<MutableStoreRequest>,
    ) -> std::result::Result<Response<MutableStoreResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let key = parse_hash(&req.key, "key")?;
        let val = parse_hash(&req.value, "value")?;
        let key_type = KeyType::try_from(req.key_type as u8)
            .map_err(|_| Status::invalid_argument("invalid key_type"))?;
        self.mutable_store
            .clone()
            .store(repo_id, key, val, key_type)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(MutableStoreResponse {}))
    }

    async fn mutable_compare_and_swap(
        &self,
        request: Request<MutableCompareAndSwapRequest>,
    ) -> std::result::Result<Response<MutableCompareAndSwapResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request)?;
        let req = request.into_inner();
        let key = parse_hash(&req.key, "key")?;
        let expected = parse_hash(&req.expected, "expected")?;
        let val = parse_hash(&req.value, "value")?;
        let key_type = KeyType::try_from(req.key_type as u8)
            .map_err(|_| Status::invalid_argument("invalid key_type"))?;
        let current = self
            .mutable_store
            .clone()
            .compare_and_swap(repo_id, key, expected, val, key_type)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(MutableCompareAndSwapResponse {
            current_value: Bytes::copy_from_slice(current.as_bytes()),
        }))
    }
}

// ============================================================================
// 5. LockService (`/urc.lock.LockService`)
// ============================================================================

#[async_trait::async_trait]
impl LockService for Arc<BitloreServerState> {
    async fn lock(
        &self,
        request: Request<LockRequest>,
    ) -> std::result::Result<Response<LockResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request).unwrap_or(Partition::ZERO);
        let req = request.into_inner();
        let mut guard = self.locks.write();
        let mut created = Vec::new();
        for res in req.resources {
            let lock = Lock {
                resource: Some(res),
                owner: "bitlore-user".to_string(),
                locked_at: None,
            };
            guard.push((repo_id, lock.clone()));
            created.push(lock);
        }
        Ok(Response::new(LockResponse { locks: created }))
    }

    async fn query(
        &self,
        request: Request<LockQueryRequest>,
    ) -> std::result::Result<Response<LockQueryResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request).unwrap_or(Partition::ZERO);
        let guard = self.locks.read();
        let result = guard
            .iter()
            .filter(|(r, _)| *r == repo_id)
            .map(|(_, l)| l.clone())
            .collect();
        Ok(Response::new(LockQueryResponse { result }))
    }

    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> std::result::Result<Response<StatusResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request).unwrap_or(Partition::ZERO);
        let req = request.into_inner();
        let guard = self.locks.read();
        let locks = guard
            .iter()
            .filter(|(r, l)| {
                *r == repo_id
                    && l.resource
                        .as_ref()
                        .is_some_and(|res| req.resources.contains(res))
            })
            .map(|(_, l)| l.clone())
            .collect();
        Ok(Response::new(StatusResponse { locks }))
    }

    async fn unlock(
        &self,
        request: Request<UnlockRequest>,
    ) -> std::result::Result<Response<UnlockResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request).unwrap_or(Partition::ZERO);
        let req = request.into_inner();
        let mut guard = self.locks.write();
        guard.retain(|(r, l)| {
            !(*r == repo_id
                && l.resource
                    .as_ref()
                    .is_some_and(|res| req.resources.contains(res)))
        });
        Ok(Response::new(UnlockResponse {
            resources: req.resources,
        }))
    }

    async fn admin_lock(
        &self,
        request: Request<AdminLockRequest>,
    ) -> std::result::Result<Response<AdminLockResponse>, Status> {
        let repo_id = repo_id_from_metadata_headers(&request).unwrap_or(Partition::ZERO);
        let req = request.into_inner();
        let mut guard = self.locks.write();
        let mut created = Vec::new();
        for res in req.resources {
            let lock = Lock {
                resource: Some(res),
                owner: req.owner.clone(),
                locked_at: None,
            };
            guard.push((repo_id, lock.clone()));
            created.push(lock);
        }
        Ok(Response::new(AdminLockResponse { locks: created }))
    }
}
