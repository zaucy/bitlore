use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    bitlore::grpc_server::run_standalone_server(bitlore::peer_store::PEER_PLUGIN_NAME).await
}
