use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context as _, anyhow, bail};
use bytes::Bytes;
use dashmap::DashMap;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, TransportAddr,
    endpoint::{Connection, presets},
};
use tracing::{debug, info, warn};
use zerocopy::IntoBytes;

use crate::local_store::{ImmutableStore, StoreGetData};
use crate::types::{Address, Context, Fragment, FragmentFlags, Hash, Partition, hash_fragment};

/// Custom QUIC ALPN identifier for Bitlore peer-to-peer chunk transfers over `iroh`.
pub const BITLORE_P2P_ALPN: &[u8] = b"/bitlore/p2p-chunk/1";

/// Maximum allowed single fragment payload size over P2P (16 MiB).
const MAX_P2P_CHUNK_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Default)]
pub struct P2pNodeStats {
    pub chunks_served: AtomicU64,
    pub bytes_served: AtomicU64,
    pub chunks_fetched: AtomicU64,
    pub bytes_fetched: AtomicU64,
}

/// An `iroh`-powered QUIC peer node that serves fragments from a local [`ImmutableStore`]
/// and fetches missing file fragments directly Peer-to-Peer from teammates.
pub struct BitloreIrohNode {
    endpoint: Endpoint,
    endpoint_addr: EndpointAddr,
    endpoint_addr_json: String,
    local_store: Arc<dyn ImmutableStore>,
    connections: DashMap<EndpointId, Connection>,
    stats: Arc<P2pNodeStats>,
}

impl BitloreIrohNode {
    /// Binds a new `iroh::Endpoint` and spawns the background QUIC P2P chunk server loop.
    pub async fn bind(
        local_store: Arc<dyn ImmutableStore>,
        use_n0_relay: bool,
    ) -> anyhow::Result<Arc<Self>> {
        let endpoint = if use_n0_relay {
            Endpoint::builder(presets::N0)
                .alpns(vec![BITLORE_P2P_ALPN.to_vec()])
                .bind()
                .await
                .map_err(|e| anyhow!("failed to bind iroh endpoint (N0 preset): {e}"))?
        } else {
            Endpoint::builder(presets::Minimal)
                .alpns(vec![BITLORE_P2P_ALPN.to_vec()])
                .bind()
                .await
                .map_err(|e| anyhow!("failed to bind iroh endpoint (Minimal preset): {e}"))?
        };

        let mut addr = endpoint.addr();
        // Ensure loopback & bound socket addresses are included so peers on the same LAN or host
        // can dial directly without waiting for external address discovery.
        let mut transport_addrs: Vec<TransportAddr> = addr.addrs.into_iter().collect();
        for sock in endpoint.bound_sockets() {
            if sock.ip().is_unspecified() {
                if sock.is_ipv4() {
                    let loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), sock.port());
                    if !transport_addrs.contains(&TransportAddr::Ip(loopback)) {
                        transport_addrs.push(TransportAddr::Ip(loopback));
                    }
                }
            } else if !transport_addrs.contains(&TransportAddr::Ip(sock)) {
                transport_addrs.push(TransportAddr::Ip(sock));
            }
        }
        addr = EndpointAddr::from_parts(endpoint.id(), transport_addrs);
        let endpoint_addr_json =
            serde_json::to_string(&addr).context("failed to serialize iroh EndpointAddr")?;

        let stats = Arc::new(P2pNodeStats::default());
        let node = Arc::new(Self {
            endpoint: endpoint.clone(),
            endpoint_addr: addr,
            endpoint_addr_json,
            local_store: local_store.clone(),
            connections: DashMap::new(),
            stats: stats.clone(),
        });

        info!(
            peer_id = %node.peer_id(),
            bound_sockets = ?endpoint.bound_sockets(),
            "Bitlore iroh P2P QUIC node bound and listening for peer chunk requests"
        );

        let accept_ep = endpoint;
        let accept_store = local_store;
        let accept_stats = stats;
        crate::spawn_bg(async move {
            Self::accept_loop(accept_ep, accept_store, accept_stats).await;
        });

        Ok(node)
    }

    pub fn peer_id(&self) -> String {
        self.endpoint.id().to_string()
    }

    pub fn endpoint_addr(&self) -> &EndpointAddr {
        &self.endpoint_addr
    }

    pub fn endpoint_addr_json(&self) -> &str {
        &self.endpoint_addr_json
    }

    pub fn local_store(&self) -> &Arc<dyn ImmutableStore> {
        &self.local_store
    }

    pub fn stats(&self) -> &Arc<P2pNodeStats> {
        &self.stats
    }

    async fn accept_loop(
        endpoint: Endpoint,
        local_store: Arc<dyn ImmutableStore>,
        stats: Arc<P2pNodeStats>,
    ) {
        while let Some(incoming) = endpoint.accept().await {
            let store = local_store.clone();
            let stats = stats.clone();
            crate::spawn_bg(async move {
                let conn = match incoming.await {
                    Ok(c) => c,
                    Err(e) => {
                        debug!(error = %e, "iroh incoming connection handshake failed");
                        return;
                    }
                };
                let remote_peer = conn.remote_id();
                debug!(%remote_peer, "Accepted incoming iroh P2P connection");

                loop {
                    let (mut send, mut recv) = match conn.accept_bi().await {
                        Ok(bi) => bi,
                        Err(_) => break,
                    };
                    let store = store.clone();
                    let stats = stats.clone();
                    crate::spawn_bg(async move {
                        if let Err(e) =
                            Self::handle_chunk_stream(&mut send, &mut recv, store, stats).await
                        {
                            debug!(error = %e, "iroh chunk stream handler error");
                        }
                    });
                }
            });
        }
    }

    async fn handle_chunk_stream(
        send: &mut iroh::endpoint::SendStream,
        recv: &mut iroh::endpoint::RecvStream,
        local_store: Arc<dyn ImmutableStore>,
        stats: Arc<P2pNodeStats>,
    ) -> anyhow::Result<()> {
        // Request frame: 16B Partition + 32B Hash + 16B Context = 64 bytes
        let mut req_buf = [0u8; 64];
        recv.read_exact(&mut req_buf)
            .await
            .map_err(|e| anyhow!("failed to read 64-byte chunk request: {e}"))?;

        let part_arr: [u8; 16] = req_buf[0..16].try_into().unwrap();
        let hash_arr: [u8; 32] = req_buf[16..48].try_into().unwrap();
        let ctx_arr: [u8; 16] = req_buf[48..64].try_into().unwrap();

        let partition = Partition::from(&part_arr);
        let address = Address {
            hash: Hash::from(hash_arr),
            context: Context::from(&ctx_arr),
        };

        match local_store
            .get(partition, address)
            .await
            .and_then(StoreGetData::into_payload)
        {
            Ok((fragment, payload)) => {
                // Response header: 1B status (1 = found) + 4B size_payload + 8B size_content + 4B flags = 17 bytes
                let mut header = [0u8; 17];
                header[0] = 1u8;
                header[1..5].copy_from_slice(&fragment.size_payload.to_le_bytes());
                header[5..13].copy_from_slice(&fragment.size_content.to_le_bytes());
                header[13..17].copy_from_slice(&fragment.flags.to_le_bytes());

                send.write_all(&header)
                    .await
                    .map_err(|e| anyhow!("failed to write P2P chunk header: {e}"))?;
                send.write_all(&payload)
                    .await
                    .map_err(|e| anyhow!("failed to write P2P chunk payload: {e}"))?;
                let _ = send.finish();

                let served = stats.chunks_served.fetch_add(1, Ordering::Relaxed) + 1;
                let total_bytes = stats
                    .bytes_served
                    .fetch_add(payload.len() as u64, Ordering::Relaxed)
                    + payload.len() as u64;
                info!(
                    %partition,
                    %address,
                    chunk_bytes = payload.len(),
                    total_chunks_served = served,
                    total_bytes_served = total_bytes,
                    "Bitlore iroh P2P: served file chunk directly to teammate peer"
                );
            }
            Err(_) => {
                send.write_all(&[0u8])
                    .await
                    .map_err(|e| anyhow!("failed to write P2P not-found byte: {e}"))?;
                let _ = send.finish();
            }
        }
        Ok(())
    }

    async fn get_or_connect(&self, peer_addr: &EndpointAddr) -> anyhow::Result<Connection> {
        if let Some(existing) = self.connections.get(&peer_addr.id) {
            if existing.close_reason().is_none() {
                return Ok(existing.clone());
            }
            drop(existing);
            self.connections.remove(&peer_addr.id);
        }

        let conn = tokio::time::timeout(
             std::time::Duration::from_secs(3),
            self.endpoint.connect(peer_addr.clone(), BITLORE_P2P_ALPN),
        )
        .await
        .map_err(|_| anyhow!("timed out connecting to iroh peer {}", peer_addr.id))?
        .map_err(|e| anyhow!("failed to connect to iroh peer {}: {e}", peer_addr.id))?;
        self.connections.insert(peer_addr.id, conn.clone());
        Ok(conn)
    }

    /// Fetches a single `(partition, address)` chunk from a remote `iroh` peer and verifies
    /// its BLAKE3 content hash against `address.hash`.
    pub async fn fetch_chunk_from_peer(
        &self,
        peer_addr: &EndpointAddr,
        partition: Partition,
        address: Address,
    ) -> anyhow::Result<(Fragment, Bytes)> {
        let conn = self.get_or_connect(peer_addr).await?;
        let (mut send, mut recv) = match conn.open_bi().await {
            Ok(bi) => bi,
            Err(_) => {
                // Reconnect once if cached connection closed
                self.connections.remove(&peer_addr.id);
                let fresh = self.get_or_connect(peer_addr).await?;
                fresh
                    .open_bi()
                    .await
                    .map_err(|e| anyhow!("failed to open bi stream to peer: {e}"))?
            }
        };

        let mut req_buf = [0u8; 64];
        req_buf[0..16].copy_from_slice(partition.as_bytes());
        req_buf[16..48].copy_from_slice(address.hash.as_bytes());
        req_buf[48..64].copy_from_slice(address.context.as_bytes());

        send.write_all(&req_buf)
            .await
            .map_err(|e| anyhow!("failed to send 64B chunk request: {e}"))?;
        let _ = send.finish();

        let mut status = [0u8; 1];
        recv.read_exact(&mut status)
            .await
            .map_err(|e| anyhow!("failed to read P2P chunk status byte: {e}"))?;
        if status[0] != 1u8 {
            bail!("peer {} does not have fragment {}", peer_addr.id, address);
        }

        let mut header_buf = [0u8; 16];
        recv.read_exact(&mut header_buf)
            .await
            .map_err(|e| anyhow!("failed to read 16B fragment header from peer: {e}"))?;

        let size_payload = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let size_content = u64::from_le_bytes(header_buf[4..12].try_into().unwrap());
        let flags = u32::from_le_bytes(header_buf[12..16].try_into().unwrap());

        if size_payload as usize > MAX_P2P_CHUNK_SIZE {
            bail!(
                "peer returned oversized chunk payload ({} bytes)",
                size_payload
            );
        }

        let mut payload_vec = vec![0u8; size_payload as usize];
        recv.read_exact(&mut payload_vec)
            .await
            .map_err(|e| anyhow!("failed to read chunk payload bytes from peer: {e}"))?;
        let payload = Bytes::from(payload_vec);

        let fragment = Fragment {
            size_payload,
            size_content,
            flags: flags | FragmentFlags::PayloadStoredDurable.bits(),
        };

        // Cryptographically verify the content hash against `address.hash` using BLAKE3
        let computed_hash = hash_fragment(fragment, &payload)
            .map_err(|e| anyhow!("peer chunk decompression failed: {e}"))?;
        if computed_hash != address.hash {
            warn!(
                peer_id = %peer_addr.id,
                expected = %address.hash,
                actual = %computed_hash,
                "Bitlore iroh P2P: BLAKE3 hash mismatch from peer! Rejecting chunk"
            );
            bail!(
                "BLAKE3 hash mismatch from peer {}: expected {}, got {}",
                peer_addr.id,
                address.hash,
                computed_hash
            );
        }

        let fetched = self.stats.chunks_fetched.fetch_add(1, Ordering::Relaxed) + 1;
        let total_bytes = self
            .stats
            .bytes_fetched
            .fetch_add(payload.len() as u64, Ordering::Relaxed)
            + payload.len() as u64;
        info!(
            from_peer = %peer_addr.id,
            %partition,
            %address,
            chunk_bytes = payload.len(),
            total_chunks_fetched = fetched,
            total_bytes_fetched = total_bytes,
            "Bitlore iroh P2P: fetched and BLAKE3-verified file chunk from teammate peer"
        );

        Ok((fragment, payload))
    }
}
