mod node_group_rpc {
    tonic::include_proto!("node_group_rpc");
}
mod node_rpc {
    tonic::include_proto!("node_rpc");
}
use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::{transport::Server, Request, Response, Status};
use tonic::transport::{Channel, Endpoint, Error, Uri};
use crate::node_group_rpc::node_group_rpc_server::{NodeGroupRpc, NodeGroupRpcServer};
use crate::node_group_rpc::{AddServerRequest, AddServerResponse, GetServerRequest, GetServerResponse};
use crate::node_rpc::node_rpc_client::{NodeRpcClient as NClient};
use crate::node_rpc::node_rpc_client::NodeRpcClient;
use crate::node_rpc::PingRequest;

type Nodes = Arc<Mutex<HashSet<SocketAddr>>>;
type Node = SocketAddr;

#[derive(Debug, Clone)]
pub struct ImplNodeGroupRpc {
    nodes: Nodes,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "NodeGroup")]
struct Opt {
    #[structopt(long, parse(try_from_str), default_value="127.0.0.1:5000")]
    listen: SocketAddr,

    #[structopt(long, parse(try_from_str), default_value="1")]
    ping_sec: u64
}

#[tonic::async_trait]
impl NodeGroupRpc for ImplNodeGroupRpc {
    async fn add_server(&self, request: Request<AddServerRequest>) -> Result<Response<AddServerResponse>, Status> {
        let req = request.into_inner();
        let mut nodes = self.nodes.lock().await;
        match req.addr.parse::<SocketAddr>() {
            Ok(socket) => {
                println!("{:?}", socket.clone());
                nodes.insert(socket.clone());
                Ok(Response::new(AddServerResponse { result: format!("Added {} to cluster", socket) }))
            },
            Err(e) => return Err(Status::invalid_argument(format!("Can't add socket to cluster: {e}"))),
        }
    }

    async fn get_server(&self, _request: Request<GetServerRequest>) -> Result<Response<GetServerResponse>, Status> {
        let mut nodes = self.nodes.lock().await;
        let servers_strings: Vec<String> = nodes.iter().map(|addr| addr.to_string()).collect();
        Ok(Response::new(GetServerResponse {
            result: servers_strings,
        }))
    }
}

impl ImplNodeGroupRpc {
    async fn ping_nodes(&self) {
        let nodes = self.nodes.lock().await;

        for node in nodes.iter().cloned() {
            let nodes_clone = self.nodes.clone();
            tokio::spawn(Self::ping_node(nodes_clone, node));
        }
    }

    async fn ping_node(nodes: Arc<Mutex<HashSet<Node>>>, node: Node) {
        let uri = match Uri::builder()
            .scheme("http")
            .authority(node.to_string())
            .path_and_query("/")
            .build()
        {
            Ok(uri) => uri,
            Err(_) => {
                Self::remove_node(&nodes, &node).await;
                return;
            }
        };

        let endpoint = match Endpoint::from_shared(uri.to_string()) {
            Ok(endpoint) => endpoint,
            Err(_) => {
                Self::remove_node(&nodes, &node).await;
                return;
            }
        };

        let mut connection = match NClient::connect(endpoint).await {
            Ok(connection) => connection,
            Err(_) => {
                Self::remove_node(&nodes, &node).await;
                return;
            }
        };

        match connection.ping(PingRequest{}).await {
            Ok(response) => {
                if response.into_inner().result != "Pong" {
                    Self::remove_node(&nodes, &node).await;
                }
            }
            Err(_) => {
                Self::remove_node(&nodes, &node).await;
            }
        }
    }

    async fn remove_node(nodes: &Arc<Mutex<HashSet<Node>>>, node: &Node) {
        let mut nodes = nodes.lock().await;
        nodes.remove(node);
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();
    let addr = opt.listen;
    let node_group_rpc = ImplNodeGroupRpc {
        nodes: Arc::new(Mutex::new(HashSet::new())),
    };

    let ng_for_pinging = node_group_rpc.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(opt.ping_sec)).await;
            println!("pinging");
            ng_for_pinging.ping_nodes().await;
        }
    });

    println!("NodeGroup listening on {}", addr);

    Server::builder()
        .add_service(NodeGroupRpcServer::new(node_group_rpc))
        .serve(addr)
        .await?;

    Ok(())
}

// tests
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{SocketAddr, TcpListener};
    use tonic::transport::{Channel, Server};
    use tonic::{Request, Response, Status};
    use node_group_rpc::node_group_rpc_client::NodeGroupRpcClient;
    use node_group_rpc::{AddServerRequest, GetServerRequest};
    use node_rpc::node_rpc_server::{NodeRpc, NodeRpcServer};
    use node_rpc::{PingRequest, PingResponse, SetRequest, SetResponse, GetRequest, GetResponse};
    use tokio::sync::Mutex;
    use tokio::time::sleep;
    use std::time::Duration;

    #[derive(Default)]
    struct MockNodeRpc;

    #[tonic::async_trait]
    impl NodeRpc for MockNodeRpc {
        async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
            Ok(Response::new(PingResponse {
                result: "Pong".into(),
            }))
        }

        async fn set(&self, _request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
            Ok(Response::new(SetResponse {
                result: "".to_string(),
            }))
        }

        async fn get(&self, _request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
            Ok(Response::new(GetResponse {
                value: "value".into(),
                found: false,
            }))
        }
    }

    async fn start_mock_node_server(addr: SocketAddr) {
        let mock_node_rpc = MockNodeRpc::default();
        Server::builder()
            .add_service(NodeRpcServer::new(mock_node_rpc))
            .serve(addr)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_ping_node_success() {
        let node_addr: SocketAddr = "127.0.0.1:6001".parse().unwrap();
        let nodes = Arc::new(Mutex::new(HashSet::new()));
        nodes.lock().await.insert(node_addr);

        tokio::spawn(start_mock_node_server(node_addr));

        sleep(Duration::from_secs(1)).await;

        ImplNodeGroupRpc::ping_node(nodes.clone(), node_addr).await;

        assert!(nodes.lock().await.contains(&node_addr));
    }

    #[tokio::test]
    async fn test_ping_node_failure() {
        let node_addr: SocketAddr = "127.0.0.1:6002".parse().unwrap();
        let nodes = Arc::new(Mutex::new(HashSet::new()));
        nodes.lock().await.insert(node_addr);

        ImplNodeGroupRpc::ping_node(nodes.clone(), node_addr).await;

        assert!(!nodes.lock().await.contains(&node_addr));
    }

    #[tokio::test]
    async fn test_ping_nodes() {
        let node_addr: SocketAddr = "127.0.0.1:6003".parse().unwrap();
        let nodes = Arc::new(Mutex::new(HashSet::new()));
        nodes.lock().await.insert(node_addr);

        let node_group_rpc = ImplNodeGroupRpc { nodes: nodes.clone() };

        tokio::spawn(start_mock_node_server(node_addr));

        sleep(Duration::from_secs(1)).await;

        node_group_rpc.ping_nodes().await;

        sleep(Duration::from_secs(1)).await;

        assert!(nodes.lock().await.contains(&node_addr));
    }

    // Existing tests...

    #[tokio::test]
    async fn test_add_server() {
        let addr = "127.0.0.1:5001".parse().unwrap();
        let node_group_rpc = ImplNodeGroupRpc {
            nodes: Arc::new(Mutex::new(HashSet::new())),
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
            nodes: Arc::new(Mutex::new(HashSet::new())),
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
        let nodes = Arc::new(Mutex::new(HashSet::new()));
        nodes.lock().await.insert("127.0.0.1:8080".parse().unwrap());
        let node_group_rpc = ImplNodeGroupRpc { nodes };

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


