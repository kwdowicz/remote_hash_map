/// This module handles the RPC interface for node groups.
pub mod node_group;
/// This module contains the generated gRPC code for node group RPC.
pub mod node_group_rpc;
/// This module contains the generated gRPC code for node RPC.
pub mod node_rpc;
/// This module provides the implementation for a remote hash map.
pub mod rhm;
/// This module handles storage-related functionalities.
pub mod storage;

use crate::node_group_rpc::{AddServerRequest, GetServerRequest};
use crate::rhm::{Rhm, RhmResult};
use node_group_rpc::node_group_rpc_client::NodeGroupRpcClient as NGClient;
use node_rpc::node_rpc_server::{NodeRpc, NodeRpcServer};
use node_rpc::{GetRequest, GetResponse, PingRequest, PingResponse, SetRequest, SetResponse};
use std::net::SocketAddr;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tonic::transport::{Endpoint, Server, Uri};
use tonic::{Request, Response, Status};
use log::{info, error};

/// Command-line options for configuring the Node service.
#[derive(StructOpt, Debug)]
#[structopt(name = "Node")]
struct Opt {
    /// The address to listen on for incoming requests.
    #[structopt(long, parse(try_from_str), default_value = "127.0.0.1:6000")]
    listen: SocketAddr,

    /// Optional address of the node group.
    #[structopt(long, parse(try_from_str))]
    ng: Option<SocketAddr>,
}

/// Implementation of the Node RPC service.
#[derive(Debug)]
pub struct ImplNodeRpc {
    /// Remote hash map for storing key-value pairs.
    rhm: Arc<Mutex<Rhm>>,
    /// Address of the node.
    addr: SocketAddr,
}

#[tonic::async_trait]
impl NodeRpc for ImplNodeRpc {
    /// Handles the set request to store a key-value pair.
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        let mut rhm = self.rhm.lock().await;
        info!("Received set request: key = {}, value = {}", req.key, req.value);
        let result = rhm
            .set(&req.key, &req.value)
            .await
            .map_err(|e| {
                error!("Failed to set value: {}", e);
                Status::internal(format!("Failed to set value: {}", e))
            })?;

        info!("Set request successful: key = {}", req.key);
        Ok(Response::new(SetResponse {
            result: result.value(),
        }))
    }

    /// Handles the get request to retrieve a value for a given key.
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let mut rhm = self.rhm.lock().await;
        info!("Received get request: key = {}", req.key);
        let result = rhm.get(&req.key).await;

        info!(
            "Get request result: key = {}, found = {}",
            req.key,
            matches!(result, RhmResult::Value(_))
        );
        Ok(Response::new(GetResponse {
            value: result.value(),
            found: matches!(result, RhmResult::Value(_)),
        }))
    }

    /// Handles the ping request to check the node's availability.
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        info!("Received ping request");
        Ok(Response::new(PingResponse {
            result: "Pong".to_string(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the logger with the default level set to 'info'.
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let opt = Opt::from_args();
    let addr = opt.listen;
    let node_rpc = ImplNodeRpc {
        rhm: Arc::new(Mutex::new(Rhm::new().await?)),
        addr,
    };

    info!("Starting node with configuration: {:?}", node_rpc);

    if let Some(ng_addr) = opt.ng {
        attach_to_group(ng_addr, node_rpc.addr).await?;
    }

    info!("Node listening on {}", addr);

    // Start the gRPC server.
    Server::builder()
        .add_service(NodeRpcServer::new(node_rpc))
        .serve(addr)
        .await?;

    Ok(())
}

/// Attaches the current node to a node group.
///
/// # Arguments
///
/// * `ng_addr` - The address of the node group.
/// * `node_addr` - The address of the current node.
///
/// # Returns
///
/// * `Result<(), Box<dyn std::error::Error>>` - Result of the operation.
async fn attach_to_group(
    ng_addr: SocketAddr,
    node_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(ng_addr.to_string())
        .path_and_query("/")
        .build()?;
    let endpoint = Endpoint::from_shared(uri.to_string())?;
    let mut client = NGClient::connect(endpoint).await?;
    client
        .add_server(AddServerRequest {
            addr: node_addr.to_string(),
        })
        .await?;
    let response = client.get_server(GetServerRequest {}).await?;
    info!("Attached to group: {:?}", response);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rhm::Rhm;
    use std::time::Duration;
    use tokio::time::sleep;

    /// Test implementation of the Node RPC service.
    pub struct TestNodeRpc {
        rhm: Arc<Mutex<Rhm>>,
        addr: SocketAddr,
    }

    #[tonic::async_trait]
    impl NodeRpc for TestNodeRpc {
        async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
            let req = request.into_inner();
            let mut rhm = self.rhm.lock().await;
            let result = rhm
                .set(&req.key, &req.value)
                .await
                .map_err(|e| Status::internal(format!("Failed to set value: {}", e)))?;

            Ok(Response::new(SetResponse {
                result: result.value(),
            }))
        }

        async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
            let req = request.into_inner();
            let mut rhm = self.rhm.lock().await;
            let result = rhm.get(&req.key).await;

            Ok(Response::new(GetResponse {
                value: result.value(),
                found: matches!(result, RhmResult::Value(_)),
            }))
        }

        async fn ping(
            &self,
            _request: Request<PingRequest>,
        ) -> Result<Response<PingResponse>, Status> {
            Ok(Response::new(PingResponse {
                result: "Pong".to_string(),
            }))
        }
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let rhm = Arc::new(Mutex::new(Rhm::new().await.unwrap()));
        let addr = "127.0.0.1:6011".parse().unwrap();
        let node_rpc = TestNodeRpc {
            rhm: rhm.clone(),
            addr,
        };

        let svc = NodeRpcServer::new(node_rpc);

        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(3)).await;

        let mut client =
            crate::node_rpc::node_rpc_client::NodeRpcClient::connect(format!("http://{}", addr))
                .await
                .unwrap();

        let set_request = tonic::Request::new(SetRequest {
            key: "key1".into(),
            value: "value1".into(),
        });
        let set_response = client.set(set_request).await.unwrap().into_inner();
        assert_eq!(set_response.result, "value1");

        let get_request = tonic::Request::new(GetRequest { key: "key1".into() });
        let get_response = client.get(get_request).await.unwrap().into_inner();
        assert_eq!(get_response.value, "value1");
    }

    #[tokio::test]
    async fn test_ping() {
        let rhm = Arc::new(Mutex::new(Rhm::new().await.unwrap()));
        let addr = "127.0.0.1:6002".parse().unwrap();
        let node_rpc = TestNodeRpc {
            rhm: rhm.clone(),
            addr,
        };

        let svc = NodeRpcServer::new(node_rpc);

        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(3)).await;

        let mut client =
            crate::node_rpc::node_rpc_client::NodeRpcClient::connect(format!("http://{}", addr))
                .await
                .unwrap();

        let ping_request = tonic::Request::new(PingRequest {});
        let ping_response = client.ping(ping_request).await.unwrap().into_inner();
        assert_eq!(ping_response.result, "Pong");
    }
}
