use std::future::Future;
use std::sync::OnceLock;

pub mod grpc_server;
pub mod local_store;
pub mod p2p_node;
pub mod peer_store;
pub mod quic_server;
pub mod store;
pub mod tracker;
pub mod types;

/// Pregenerated Tonic/Prost gRPC definitions for the Lore wire protocol.
pub mod lore {
    pub mod model {
        pub mod v1 {
            include!("grpc/lore.model.v1.rs");
        }
    }
    pub mod environment {
        pub mod v1 {
            include!("grpc/lore.environment.v1.rs");
        }
    }
    pub mod repository {
        pub mod v1 {
            include!("grpc/lore.repository.v1.rs");
        }
    }
    pub mod revision {
        pub mod v1 {
            include!("grpc/lore.revision.v1.rs");
        }
    }
    pub mod storage {
        pub mod v1 {
            include!("grpc/lore.storage.v1.rs");
        }
    }
}

pub mod urc {
    pub mod lock {
        include!("grpc/urc.lock.rs");
    }
}

fn fallback_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build bitlore background tokio runtime")
    })
}

/// Spawns a background task onto the currently active Tokio runtime if one is entered,
/// or onto a shared multi-threaded fallback runtime otherwise.
pub fn spawn_bg<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle.spawn(future),
        Err(_) => fallback_runtime().spawn(future),
    }
}
