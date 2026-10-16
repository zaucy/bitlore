use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    bitlore::grpc_server::run_standalone_server(bitlore::store::PLUGIN_NAME).await
}
