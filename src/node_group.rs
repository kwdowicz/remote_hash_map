pub mod node_group_rpc {
    tonic::include_proto!("node_group_rpc");
}

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};
use node_group_rpc::node_group_rpc_server::{NodeGroupRpc, NodeGroupRpcServer};
use node_group_rpc::{AddServerRequest, AddServerResponse, GetServerRequest, GetServerResponse};

#[derive(Debug)]
pub struct ImplNodeGroupRpc {
    servers: Arc<Mutex<HashSet<SocketAddr>>>,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "Server")]
struct Opt {
    #[structopt(long, parse(try_from_str), default_value="127.0.0.1:5000")]
    listen: SocketAddr,
}

#[tonic::async_trait]
impl NodeGroupRpc for ImplNodeGroupRpc {
    async fn add_server(&self, request: Request<AddServerRequest>) -> Result<Response<AddServerResponse>, Status> {
        let req = request.into_inner();
        let mut servers = self.servers.lock().await;
        match req.addr.parse::<SocketAddr>() {
            Ok(socket) => {
                servers.insert(socket.clone());
                Ok(Response::new(AddServerResponse { result: format!("Added {} to cluster", socket) }))

            },
            Err(e) => return Err(Status::invalid_argument(format!("Can't add socket to cluster: {e}"))),
        }
    }

    async fn get_server(&self, _request: Request<GetServerRequest>) -> Result<Response<GetServerResponse>, Status> {
        let mut servers = self.servers.lock().await;
        let servers_strings: Vec<String> = servers.iter().map(|addr| addr.to_string()).collect();
        Ok(Response::new(GetServerResponse {
            result: servers_strings,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();
    let addr = opt.listen;
    let node_group_rpc = ImplNodeGroupRpc {
        servers: Arc::new(Mutex::new(HashSet::new())),
    };

    println!("Server listening on {}", addr);

    Server::builder()
        .add_service(NodeGroupRpcServer::new(node_group_rpc))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use super::*;
    use tokio::sync::Mutex;
    use tokio::time::sleep;
    use tonic::transport::{Server, Channel};
    use tonic::Request;
    use node_group_rpc::node_group_rpc_client::NodeGroupRpcClient;
    use node_group_rpc::{AddServerRequest, GetServerRequest};

    #[tokio::test]
    async fn test_add_server() {
        let addr = "127.0.0.1:5001".parse().unwrap();
        let node_group_rpc = ImplNodeGroupRpc {
            servers: Arc::new(Mutex::new(HashSet::new())),
        };

        tokio::spawn(async move {
            Server::builder()
                .add_service(NodeGroupRpcServer::new(node_group_rpc))
                .serve(addr)
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(3)).await;

        let channel = Channel::from_static("http://127.0.0.1:5001")
            .connect()
            .await
            .unwrap();
        let mut client = NodeGroupRpcClient::new(channel);

        let request = tonic::Request::new(AddServerRequest {
            addr: "127.0.0.1:8080".to_string(),
        });

        let response = client.add_server(request).await.unwrap();
        assert_eq!(response.into_inner().result, "Added 127.0.0.1:8080 to cluster");
    }

    #[tokio::test]
    async fn test_add_server_invalid_address() {
        let addr = "127.0.0.1:5002".parse().unwrap();
        let node_group_rpc = ImplNodeGroupRpc {
            servers: Arc::new(Mutex::new(HashSet::new())),
        };

        tokio::spawn(async move {
            Server::builder()
                .add_service(NodeGroupRpcServer::new(node_group_rpc))
                .serve(addr)
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(3)).await;

        let channel = Channel::from_static("http://127.0.0.1:5002")
            .connect()
            .await
            .unwrap();
        let mut client = NodeGroupRpcClient::new(channel);

        let request = tonic::Request::new(AddServerRequest {
            addr: "invalid_address".to_string(),
        });

        let response = client.add_server(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_get_server() {
        let addr = "127.0.0.1:5003".parse().unwrap();
        let servers = Arc::new(Mutex::new(HashSet::new()));
        servers.lock().await.insert("127.0.0.1:8080".parse().unwrap());
        let node_group_rpc = ImplNodeGroupRpc { servers };

        tokio::spawn(async move {
            Server::builder()
                .add_service(NodeGroupRpcServer::new(node_group_rpc))
                .serve(addr)
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(3)).await;

        let channel = Channel::from_static("http://127.0.0.1:5003")
            .connect()
            .await
            .unwrap();
        let mut client = NodeGroupRpcClient::new(channel);

        let request = tonic::Request::new(GetServerRequest {});
        let response = client.get_server(request).await.unwrap();
        let result = response.into_inner().result;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "127.0.0.1:8080");
    }
}