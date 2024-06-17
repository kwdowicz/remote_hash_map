mod node_group_rpc {
    tonic::include_proto!("node_group_rpc");
}

mod node_rpc {
    tonic::include_proto!("node_rpc");
}

use crate::node_group_rpc::node_group_rpc_server::{NodeGroupRpc, NodeGroupRpcServer};
use crate::node_group_rpc::{AddServerRequest, AddServerResponse, GetServerRequest, GetServerResponse, ReplicateRequest, ReplicateResponse};
use crate::node_rpc::node_rpc_client::NodeRpcClient as NClient;
use crate::node_rpc::node_rpc_client::NodeRpcClient;
use crate::node_rpc::{PingRequest, SetRequest};
use log::{error, info};
use std::collections::HashSet;
use std::fmt::Debug;
use std::net::{AddrParseError, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint, Error, Uri};
use tonic::{transport::Server, Request, Response, Status};

type Nodes = Arc<Mutex<HashSet<SocketAddr>>>;

type Node = SocketAddr;

#[derive(Debug, Clone)]
pub struct ImplNodeGroupRpc {
    nodes: Nodes,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "NodeGroup")]
struct Opt {
    #[structopt(long, parse(try_from_str), default_value = "127.0.0.1:5000")]
    listen: SocketAddr,

    #[structopt(long, parse(try_from_str), default_value = "30")]
    ping_sec: u64,
}

#[tonic::async_trait]
impl NodeGroupRpc for ImplNodeGroupRpc {
    async fn add_server(&self, request: Request<AddServerRequest>) -> Result<Response<AddServerResponse>, Status> {
        let req = request.into_inner();
        let mut nodes = self.nodes.lock().await;
        match req.addr.parse::<SocketAddr>() {
            Ok(socket) => {
                info!("Adding node: {:?}", socket);
                nodes.insert(socket.clone());
                Ok(Response::new(AddServerResponse {
                    result: format!("Added {} to node group", socket),
                }))
            }
            Err(e) => {
                error!("Failed to add server: {}", e);
                return Err(Status::invalid_argument(format!("Can't add node to node group: {e}")));
            }
        }
    }

    async fn get_server(&self, _request: Request<GetServerRequest>) -> Result<Response<GetServerResponse>, Status> {
        let mut nodes = self.nodes.lock().await;
        let servers_strings: Vec<String> = nodes.iter().map(|addr| addr.to_string()).collect();
        info!("Retrieved servers: {:?}", servers_strings);
        Ok(Response::new(GetServerResponse { result: servers_strings }))
    }

    async fn replicate(&self, request: Request<ReplicateRequest>) -> Result<Response<ReplicateResponse>, Status> {
        info!("Received replication request: {:?}", request);

        let mut nodes = self.nodes.lock().await;
        let request = request.into_inner();
        let source_addr: SocketAddr = match request.source.parse() {
            Ok(addr) => addr,
            Err(_) => return Err(Status::invalid_argument("Invalid source address")),
        };

        let nodes_to_replicate: Vec<String> = nodes.iter()
            .map(|addr| addr.to_string())
            .filter(|addr| *addr != source_addr.to_string())
            .collect();

        info!("Nodes to replicate to: {:?}", nodes_to_replicate);

        for node in nodes_to_replicate.iter() {
            let uri = match Uri::builder().scheme("http").authority(node.as_str()).path_and_query("/").build() {
                Ok(uri) => uri,
                Err(e) => {
                    error!("Failed to build URI: {:?}", e);
                    continue;
                },
            };

            let endpoint = match Endpoint::from_shared(uri.to_string()) {
                Ok(endpoint) => endpoint,
                Err(e) => {
                    error!("Failed to create endpoint: {:?}", e);
                    continue;
                },
            };

            match NClient::connect(endpoint).await {
                Ok(mut client) => {
                    if let Err(e) = client.set(SetRequest {
                        key: request.key.clone(),
                        value: request.value.clone(),
                        replication: true,
                    }).await {
                        error!("Failed to set key-value pair: {:?}", e);
                    }
                },
                Err(e) => {
                    error!("Failed to connect to node {}: {:?}", node, e);
                    if let Ok(socket) = SocketAddr::from_str(node) {
                        nodes.retain(|&x| x != socket);
                    } else {
                        error!("Failed to parse node address: {}", node);
                    }
                }
            }
        }

        Ok(Response::new(ReplicateResponse {}))
    }
}

impl ImplNodeGroupRpc {
    async fn ping_nodes(&self) {
        let nodes = self.nodes.lock().await;
        info!("Pinging node(s): {:?}", nodes);

        for node in nodes.iter().cloned() {
            let nodes_clone = self.nodes.clone();
            tokio::spawn(Self::ping_node(nodes_clone, node));
        }
    }

    async fn ping_node(nodes: Arc<Mutex<HashSet<Node>>>, node: Node) {
        let uri = match Uri::builder().scheme("http").authority(node.to_string()).path_and_query("/").build() {
            Ok(uri) => uri,
            Err(_) => {
                error!("Failed to build URI for node: {}", node);
                Self::remove_node(&nodes, &node).await;
                return;
            }
        };

        let endpoint = match Endpoint::from_shared(uri.to_string()) {
            Ok(endpoint) => endpoint,
            Err(_) => {
                error!("Failed to create endpoint for node: {}", node);
                Self::remove_node(&nodes, &node).await;
                return;
            }
        };

        let mut connection = match NClient::connect(endpoint).await {
            Ok(connection) => connection,
            Err(_) => {
                error!("Failed to connect to node: {}", node);
                Self::remove_node(&nodes, &node).await;
                return;
            }
        };

        match connection.ping(PingRequest {}).await {
            Ok(response) => {
                if response.into_inner().result != "Pong" {
                    error!("Invalid ping response from node: {}", node);
                    Self::remove_node(&nodes, &node).await;
                }
            }
            Err(_) => {
                error!("Failed to ping node: {}", node);
                Self::remove_node(&nodes, &node).await;
            }
        }
    }

    async fn remove_node(nodes: &Arc<Mutex<HashSet<Node>>>, node: &Node) {
        let mut nodes = nodes.lock().await;
        nodes.remove(node);
        info!("Removed node: {}", node);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_default_env().filter_level(log::LevelFilter::Info).init();

    let opt = Opt::from_args();
    let addr = opt.listen;
    let node_group_rpc = ImplNodeGroupRpc {
        nodes: Arc::new(Mutex::new(HashSet::new())),
    };

    // Spawn a task to ping nodes at regular intervals.
    let ng_for_pinging = node_group_rpc.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(opt.ping_sec)).await;
            ng_for_pinging.ping_nodes().await;
        }
    });

    info!("NodeGroup listening on {}", addr);

    // Start the gRPC server.
    Server::builder().add_service(NodeGroupRpcServer::new(node_group_rpc)).serve(addr).await?;

    Ok(())
}
