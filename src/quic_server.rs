use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use quinn::{Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig, VarInt};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::sync::Mutex;
use tracing::{debug, warn};
use zerocopy::IntoBytes;

use crate::local_store::{CopyBehavior, ImmutableStore, MutableStore, StoreError, StoreMatch};
use crate::types::{
    Address, Context, FRAGMENT_SIZE_THRESHOLD, Fragment, FragmentFlags, Hash, KeyType, Partition,
    RepositoryId, hash_fragment, validate_fragment_list, validate_fragment_metadata,
};

pub const ALPN_STORAGE_V4: &[u8] = b"lore-storage/0.4";
pub const ALPN_STORAGE_V2: &[u8] = b"urc/0.2";

pub const COMMAND_HEADER_SIZE_V2: usize = 8;
pub const COMMAND_HEADER_SIZE_V4: usize = 12;
pub const MAX_CHUNK_SIZE: usize = FRAGMENT_SIZE_THRESHOLD + 12 + 48 + 16;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuicCommand {
    Authorize = 0,
    Get = 1,
    Put = 2,
    Query = 3,
    CorrelateLegacy = 5,
    Verify = 6,
    Copy = 7,
    MutableLoad = 8,
    MutableStore = 9,
    MutableCas = 10,
    GetMetadata = 11,
    GetResolved = 12,
    PutResolved = 13,
    ClientIdentify = 14,
}

impl TryFrom<u8> for QuicCommand {
    type Error = u8;

    fn try_from(v: u8) -> std::result::Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Authorize),
            1 => Ok(Self::Get),
            2 => Ok(Self::Put),
            3 => Ok(Self::Query),
            5 => Ok(Self::CorrelateLegacy),
            6 => Ok(Self::Verify),
            7 => Ok(Self::Copy),
            8 => Ok(Self::MutableLoad),
            9 => Ok(Self::MutableStore),
            10 => Ok(Self::MutableCas),
            11 => Ok(Self::GetMetadata),
            12 => Ok(Self::GetResolved),
            13 => Ok(Self::PutResolved),
            14 => Ok(Self::ClientIdentify),
            other => Err(other),
        }
    }
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuicServiceError {
    InvalidCommand = 1,
    NotAuthorized = 2,
    Failed = 3,
    NotFound = 4,
    Oversized = 5,
    SlowDown = 100,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct CommandHeader {
    pub cmd: u8,
    pub error: bool,
    pub size_or_status: u32,
    pub command_id: u32,
    pub session_id: u32,
    pub v4: bool,
}

impl CommandHeader {
    pub fn from_bytes(buf: &[u8], v4: bool) -> Self {
        let cmd = buf[0];
        let size_or_status =
            (buf[1] as u32) | ((buf[2] as u32) << 8) | (((buf[3] & 0x7F) as u32) << 16);
        let error = (buf[3] & 0x80) != 0;
        let command_id = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let session_id = if v4 && buf.len() >= 12 {
            u32::from_le_bytes(buf[8..12].try_into().unwrap())
        } else {
            0
        };
        Self {
            cmd,
            error,
            size_or_status,
            command_id,
            session_id,
            v4,
        }
    }

    pub fn response_success(&self, size: u32) -> Self {
        Self {
            cmd: self.cmd,
            error: false,
            size_or_status: size,
            command_id: self.command_id,
            session_id: self.session_id,
            v4: self.v4,
        }
    }

    pub fn response_error(&self, status: QuicServiceError) -> Self {
        Self {
            cmd: self.cmd,
            error: true,
            size_or_status: status as u32,
            command_id: self.command_id,
            session_id: self.session_id,
            v4: self.v4,
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let len = if self.v4 {
            COMMAND_HEADER_SIZE_V4
        } else {
            COMMAND_HEADER_SIZE_V2
        };
        let mut buf = vec![0u8; len];
        buf[0] = self.cmd;
        buf[1] = (self.size_or_status & 0xFF) as u8;
        buf[2] = ((self.size_or_status >> 8) & 0xFF) as u8;
        buf[3] = (((self.size_or_status >> 16) & 0x7F) as u8) | if self.error { 0x80 } else { 0 };
        buf[4..8].copy_from_slice(&self.command_id.to_le_bytes());
        if self.v4 {
            buf[8..12].copy_from_slice(&self.session_id.to_le_bytes());
        }
        buf
    }
}

struct QuicConnectionState {
    v4: bool,
    next_session_id: AtomicU32,
    sessions: DashMap<u32, RepositoryId>,
    v2_repository: parking_lot::RwLock<Option<RepositoryId>>,
    immutable_store: Arc<dyn ImmutableStore>,
    mutable_store: Arc<dyn MutableStore>,
}

impl QuicConnectionState {
    fn new(
        v4: bool,
        immutable_store: Arc<dyn ImmutableStore>,
        mutable_store: Arc<dyn MutableStore>,
    ) -> Self {
        Self {
            v4,
            next_session_id: AtomicU32::new(1),
            sessions: DashMap::new(),
            v2_repository: parking_lot::RwLock::new(None),
            immutable_store,
            mutable_store,
        }
    }

    fn resolve_repository(&self, session_id: u32) -> Result<RepositoryId, QuicServiceError> {
        if self.v4 {
            self.sessions
                .get(&session_id)
                .map(|r| *r.value())
                .ok_or(QuicServiceError::Failed)
        } else {
            self.v2_repository
                .read()
                .ok_or(QuicServiceError::Failed)
        }
    }
}

pub fn build_quinn_server_config() -> Result<ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .context("failed to generate self-signed TLS cert for QUIC server")?;

    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls_config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("failed to configure TLS 1.3")?
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .context("failed to set QUIC server TLS certificate")?;

    tls_config.alpn_protocols = vec![ALPN_STORAGE_V4.to_vec(), ALPN_STORAGE_V2.to_vec()];
    tls_config.max_early_data_size = u32::MAX;

    let quic_tls = quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
        .map_err(|e| anyhow!("failed to build QuicServerConfig: {e}"))?;

    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_tls));
    let mut transport = TransportConfig::default();
    transport.max_concurrent_bidi_streams(VarInt::from_u32(1024));
    transport.max_concurrent_uni_streams(VarInt::from_u32(1024));
    transport.max_idle_timeout(Some(
        Duration::from_secs(60)
            .try_into()
            .expect("valid idle timeout"),
    ));
    transport.keep_alive_interval(Some(Duration::from_secs(15)));
    server_config.transport_config(Arc::new(transport));

    Ok(server_config)
}

pub fn spawn_quic_storage_server(
    bind_addr: SocketAddr,
    immutable_store: Arc<dyn ImmutableStore>,
    mutable_store: Arc<dyn MutableStore>,
) -> Result<SocketAddr> {
    let server_config = build_quinn_server_config()?;
    let endpoint = Endpoint::server(server_config, bind_addr)
        .with_context(|| format!("failed to bind QUIC UDP endpoint on {bind_addr}"))?;
    let local_addr = endpoint.local_addr()?;

    crate::spawn_bg(async move {
        while let Some(incoming) = endpoint.accept().await {
            let immut = immutable_store.clone();
            let mut_store = mutable_store.clone();
            crate::spawn_bg(async move {
                let conn = match incoming.await {
                    Ok(c) => c,
                    Err(e) => {
                        debug!(error = %e, "QUIC incoming handshake failed");
                        return;
                    }
                };
                let alpn = conn
                    .handshake_data()
                    .and_then(|hd| hd.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
                    .and_then(|hd| hd.protocol)
                    .unwrap_or_default();
                let v4 = alpn.as_slice() != ALPN_STORAGE_V2;
                let conn_state = Arc::new(QuicConnectionState::new(v4, immut, mut_store));

                loop {
                    let (send, recv) = match conn.accept_bi().await {
                        Ok(bi) => bi,
                        Err(_) => break,
                    };
                    let state = conn_state.clone();
                    crate::spawn_bg(async move {
                        let _ = handle_quic_stream(send, recv, state).await;
                    });
                }
            });
        }
    });

    Ok(local_addr)
}

async fn handle_quic_stream(
    send: SendStream,
    mut recv: RecvStream,
    state: Arc<QuicConnectionState>,
) -> Result<()> {
    let send = Arc::new(Mutex::new(send));
    let header_size = if state.v4 {
        COMMAND_HEADER_SIZE_V4
    } else {
        COMMAND_HEADER_SIZE_V2
    };
    let mut buf = BytesMut::with_capacity(64 * 1024);

    loop {
        while buf.len() >= header_size {
            let header = CommandHeader::from_bytes(&buf[..header_size], state.v4);
            if header.error {
                return Ok(());
            }
            let payload_len = header.size_or_status as usize;
            if buf.len() < header_size + payload_len {
                break;
            }
            let _ = buf.split_to(header_size);
            let payload = buf.split_to(payload_len).freeze();

            let send_clone = send.clone();
            let state_clone = state.clone();
            crate::spawn_bg(async move {
                let res = dispatch_quic_command(header, payload, &state_clone).await;
                let out_bytes = match res {
                    Ok(chunks) => {
                        let total_len: usize = chunks.iter().map(Bytes::len).sum();
                        let resp_hdr = header.response_success(total_len as u32).to_vec();
                        let mut full = Vec::with_capacity(resp_hdr.len() + total_len);
                        full.extend_from_slice(&resp_hdr);
                        for c in chunks {
                            full.extend_from_slice(&c);
                        }
                        full
                    }
                    Err(err_code) => header.response_error(err_code).to_vec(),
                };
                let mut guard = send_clone.lock().await;
                let _ = guard.write_all(&out_bytes).await;
            });
        }

        match recv.read_chunk(MAX_CHUNK_SIZE, true).await {
            Ok(Some(chunk)) => {
                buf.extend_from_slice(&chunk.bytes);
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    Ok(())
}

fn map_store_err(err: StoreError) -> QuicServiceError {
    match err {
        StoreError::AddressNotFound(_) | StoreError::PayloadNotFound => QuicServiceError::NotFound,
        StoreError::SlowDown(_) => QuicServiceError::SlowDown,
        _ => QuicServiceError::Failed,
    }
}

async fn dispatch_quic_command(
    header: CommandHeader,
    payload: Bytes,
    state: &QuicConnectionState,
) -> std::result::Result<Vec<Bytes>, QuicServiceError> {
    let Ok(cmd) = QuicCommand::try_from(header.cmd) else {
        return Err(QuicServiceError::InvalidCommand);
    };

    match cmd {
        QuicCommand::ClientIdentify | QuicCommand::CorrelateLegacy => Ok(vec![]),
        QuicCommand::Authorize => {
            if state.v4 {
                if payload.is_empty() {
                    return Err(QuicServiceError::InvalidCommand);
                }
                match payload[0] {
                    0 => {
                        // Start session: action(1) + repo(16) + corr_len(1) + corr + token_len(2) + token
                        if payload.len() < 20 || header.session_id != 0 {
                            return Err(QuicServiceError::InvalidCommand);
                        }
                        let repo = RepositoryId::from(&payload[1..17]);
                        let session_id = state.next_session_id.fetch_add(1, Ordering::Relaxed);
                        state.sessions.insert(session_id, repo);
                        Ok(vec![Bytes::copy_from_slice(&session_id.to_le_bytes())])
                    }
                    1 => {
                        // Stop session
                        if payload.len() != 1 || header.session_id == 0 {
                            return Err(QuicServiceError::InvalidCommand);
                        }
                        state.sessions.remove(&header.session_id);
                        Ok(vec![])
                    }
                    _ => Err(QuicServiceError::InvalidCommand),
                }
            } else {
                if payload.len() < 16 {
                    return Err(QuicServiceError::InvalidCommand);
                }
                let repo = RepositoryId::from(&payload[0..16]);
                *state.v2_repository.write() = Some(repo);
                Ok(vec![])
            }
        }
        QuicCommand::Get => {
            let repo = state.resolve_repository(header.session_id)?;
            if payload.len() < 48 {
                return Err(QuicServiceError::InvalidCommand);
            }
            let address = Address::from(&payload[0..48]);
            let (mut fragment, data) = state
                .immutable_store
                .clone()
                .get(repo, address)
                .await
                .and_then(|d| d.into_payload())
                .map_err(map_store_err)?;
            fragment.flags &= !FragmentFlags::PayloadStored.bits();
            fragment.flags |= FragmentFlags::PayloadStoredDurable.bits();
            Ok(vec![Bytes::copy_from_slice(fragment.as_bytes()), data])
        }
        QuicCommand::GetMetadata => {
            let repo = state.resolve_repository(header.session_id)?;
            if payload.len() < 48 {
                return Err(QuicServiceError::InvalidCommand);
            }
            let address = Address::from(&payload[0..48]);
            let data = state
                .immutable_store
                .clone()
                .get_metadata(repo, address)
                .await
                .map_err(map_store_err)?;
            if data.match_made == StoreMatch::MatchNone {
                return Err(QuicServiceError::NotFound);
            }
            let mut fragment = data.fragment;
            fragment.flags &= !FragmentFlags::PayloadStored.bits();
            fragment.flags |= FragmentFlags::PayloadStoredDurable.bits();
            Ok(vec![Bytes::copy_from_slice(fragment.as_bytes())])
        }
        QuicCommand::Put => {
            let repo = state.resolve_repository(header.session_id)?;
            if payload.len() < 64 {
                return Err(QuicServiceError::InvalidCommand);
            }
            let address = Address::from(&payload[0..48]);
            let mut fragment = Fragment::from(&payload[48..64]);
            let body = payload.slice(64..);
            if body.len() != fragment.size_payload as usize {
                return Err(QuicServiceError::InvalidCommand);
            }
            validate_fragment_metadata(&fragment).map_err(|_| QuicServiceError::InvalidCommand)?;
            if (fragment.flags & FragmentFlags::PayloadFragmented.bits()) != 0 {
                validate_fragment_list(&fragment, &body)
                    .map_err(|_| QuicServiceError::Failed)?;
            }
            let computed = hash_fragment(fragment, &body).map_err(|_| QuicServiceError::Failed)?;
            if computed != address.hash {
                warn!(%address, %computed, "QUIC Put hash mismatch");
                return Err(QuicServiceError::Failed);
            }
            fragment.flags &= !FragmentFlags::PayloadStored.bits();
            let data_opt = if body.is_empty() { None } else { Some(body) };
            state
                .immutable_store
                .clone()
                .put(repo, address, fragment, data_opt, false)
                .await
                .map_err(map_store_err)?;
            Ok(vec![])
        }
        QuicCommand::Query => {
            let repo = state.resolve_repository(header.session_id)?;
            if !payload.len().is_multiple_of(48) {
                return Err(QuicServiceError::InvalidCommand);
            }
            let count = payload.len() / 48;
            let mut addresses = Vec::with_capacity(count);
            for i in 0..count {
                addresses.push(Address::from(&payload[i * 48..(i + 1) * 48]));
            }
            let mut resolved = vec![Default::default(); count];
            state
                .immutable_store
                .clone()
                .query(repo, &addresses, &mut resolved)
                .await
                .map_err(map_store_err)?;
            let status_bytes: Vec<u8> = resolved
                .iter()
                .map(|r| match r.match_made {
                    StoreMatch::MatchFull => 0u8,
                    StoreMatch::MatchPartition => 1u8,
                    StoreMatch::MatchNone | StoreMatch::MatchHash => 3u8,
                })
                .collect();
            Ok(vec![Bytes::from(status_bytes)])
        }
        QuicCommand::Verify => {
            let repo = state.resolve_repository(header.session_id)?;
            if payload.len() < 49 {
                return Err(QuicServiceError::InvalidCommand);
            }
            let address = Address::from(&payload[0..48]);
            state
                .immutable_store
                .clone()
                .get(repo, address)
                .await
                .map_err(map_store_err)?;
            Ok(vec![Bytes::from_static(&[0u8, 0u8])])
        }
        QuicCommand::Copy => {
            let repo = state.resolve_repository(header.session_id)?;
            let (source_repo, source_addr, target_ctx) = if state.v4 {
                if payload.len() != 80 {
                    return Err(QuicServiceError::InvalidCommand);
                }
                let src_repo = Partition::from(&payload[0..16]);
                let hash = Hash::from(&payload[16..48]);
                let ctx = Context::from(&payload[48..64]);
                let target_ctx = Context::from(&payload[64..80]);
                (src_repo, Address { hash, context: ctx }, target_ctx)
            } else {
                if payload.len() != 64 {
                    return Err(QuicServiceError::InvalidCommand);
                }
                let src_repo = Partition::from(&payload[0..16]);
                let hash = Hash::from(&payload[16..48]);
                let ctx = Context::from(&payload[48..64]);
                (src_repo, Address { hash, context: ctx }, ctx)
            };
            state
                .immutable_store
                .clone()
                .copy(
                    source_repo,
                    source_addr,
                    repo,
                    target_ctx,
                    CopyBehavior {
                        durable: true,
                        do_not_replicate: false,
                    },
                )
                .await
                .map_err(map_store_err)?;
            Ok(vec![])
        }
        QuicCommand::MutableLoad => {
            let repo = state.resolve_repository(header.session_id)?;
            if payload.len() < 33 {
                return Err(QuicServiceError::InvalidCommand);
            }
            let key = Hash::from(&payload[0..32]);
            let key_type =
                KeyType::try_from(payload[32]).map_err(|_| QuicServiceError::InvalidCommand)?;
            let val = state
                .mutable_store
                .clone()
                .load(repo, key, key_type)
                .await
                .map_err(map_store_err)?;
            Ok(vec![Bytes::copy_from_slice(val.as_bytes())])
        }
        QuicCommand::MutableStore => {
            let repo = state.resolve_repository(header.session_id)?;
            if payload.len() < 65 {
                return Err(QuicServiceError::InvalidCommand);
            }
            let key = Hash::from(&payload[0..32]);
            let val = Hash::from(&payload[32..64]);
            let key_type =
                KeyType::try_from(payload[64]).map_err(|_| QuicServiceError::InvalidCommand)?;
            if matches!(
                key_type,
                KeyType::RepositoryMetadata
                    | KeyType::BranchMetadata
                    | KeyType::BranchLatestPointer
            ) {
                return Err(QuicServiceError::Failed);
            }
            state
                .mutable_store
                .clone()
                .store(repo, key, val, key_type)
                .await
                .map_err(map_store_err)?;
            Ok(vec![])
        }
        QuicCommand::MutableCas => {
            let repo = state.resolve_repository(header.session_id)?;
            if payload.len() < 97 {
                return Err(QuicServiceError::InvalidCommand);
            }
            let key = Hash::from(&payload[0..32]);
            let expected = Hash::from(&payload[32..64]);
            let val = Hash::from(&payload[64..96]);
            let key_type =
                KeyType::try_from(payload[96]).map_err(|_| QuicServiceError::InvalidCommand)?;
            if matches!(
                key_type,
                KeyType::RepositoryMetadata
                    | KeyType::BranchMetadata
                    | KeyType::BranchLatestPointer
            ) {
                return Err(QuicServiceError::Failed);
            }
            let current = state
                .mutable_store
                .clone()
                .compare_and_swap(repo, key, expected, val, key_type)
                .await
                .map_err(map_store_err)?;
            Ok(vec![Bytes::copy_from_slice(current.as_bytes())])
        }
        QuicCommand::GetResolved => {
            let repo = state.resolve_repository(header.session_id)?;
            if payload.len() < 52 {
                return Err(QuicServiceError::InvalidCommand);
            }
            let key = Hash::from(&payload[0..32]);
            let context = Context::from(&payload[32..48]);
            let flags = u32::from_le_bytes(payload[48..52].try_into().unwrap());
            if flags != 0 {
                return Err(QuicServiceError::Failed);
            }
            let resolved = state
                .mutable_store
                .clone()
                .load(repo, key, KeyType::Resolve)
                .await
                .map_err(map_store_err)?;
            let (mut fragment, data) = state
                .immutable_store
                .clone()
                .get(
                    repo,
                    Address {
                        hash: resolved,
                        context,
                    },
                )
                .await
                .and_then(|d| d.into_payload())
                .map_err(map_store_err)?;
            fragment.flags &= !FragmentFlags::PayloadStored.bits();
            fragment.flags |= FragmentFlags::PayloadStoredDurable.bits();
            Ok(vec![
                Bytes::copy_from_slice(resolved.as_bytes()),
                Bytes::copy_from_slice(fragment.as_bytes()),
                data,
            ])
        }
        QuicCommand::PutResolved => {
            let repo = state.resolve_repository(header.session_id)?;
            if payload.len() < 96 {
                return Err(QuicServiceError::InvalidCommand);
            }
            let key = Hash::from(&payload[0..32]);
            if key.is_zero() {
                return Err(QuicServiceError::InvalidCommand);
            }
            let address = Address::from(&payload[32..80]);
            let mut fragment = Fragment::from(&payload[80..96]);
            let body = payload.slice(96..);

            if !address.hash.is_zero() {
                if body.is_empty() || body.len() != fragment.size_payload as usize {
                    return Err(QuicServiceError::InvalidCommand);
                }
                validate_fragment_metadata(&fragment)
                    .map_err(|_| QuicServiceError::InvalidCommand)?;
                let computed =
                    hash_fragment(fragment, &body).map_err(|_| QuicServiceError::Failed)?;
                if computed != address.hash {
                    return Err(QuicServiceError::Failed);
                }
                fragment.flags &= !FragmentFlags::PayloadStored.bits();
                state
                    .immutable_store
                    .clone()
                    .put(repo, address, fragment, Some(body), false)
                    .await
                    .map_err(map_store_err)?;
            }

            state
                .mutable_store
                .clone()
                .store(repo, key, address.hash, KeyType::Resolve)
                .await
                .map_err(map_store_err)?;
            Ok(vec![])
        }
    }
}
