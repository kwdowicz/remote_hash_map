mod node_group_rpc {
    tonic::include_proto!("node_group_rpc");
}

mod node_rpc {
    tonic::include_proto!("node_rpc");
}

use crate::node_group_rpc::node_group_rpc_server::{NodeGroupRpc, NodeGroupRpcServer};
use crate::node_group_rpc::{
    AddServerRequest, AddServerResponse, GetServerRequest, GetServerResponse,
};
use crate::node_rpc::node_rpc_client::NodeRpcClient as NClient;
use crate::node_rpc::node_rpc_client::NodeRpcClient;
use crate::node_rpc::PingRequest;
use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint, Error, Uri};
use tonic::{transport::Server, Request, Response, Status};
use log::{info, error};

/// Type alias for a thread-safe shared set of nodes.
type Nodes = Arc<Mutex<HashSet<SocketAddr>>>;
/// Type alias for a single node, represented as a socket address.
type Node = SocketAddr;

/// The main implementation of the NodeGroup RPC service.
#[derive(Debug, Clone)]
pub struct ImplNodeGroupRpc {
    /// The set of nodes managed by this service.
    nodes: Nodes,
}

/// Command-line options for configuring the NodeGroup service.
#[derive(StructOpt, Debug)]
#[structopt(name = "NodeGroup")]
struct Opt {
    /// The address to listen on for incoming requests.
    #[structopt(long, parse(try_from_str), default_value = "127.0.0.1:5000")]
    listen: SocketAddr,

    /// The interval in seconds between pings to nodes.
    #[structopt(long, parse(try_from_str), default_value = "1")]
    ping_sec: u64,
}

#[tonic::async_trait]
impl NodeGroupRpc for ImplNodeGroupRpc {
    /// Adds a server to the cluster.
    ///
    /// # Arguments
    ///
    /// * `request` - A `Request<AddServerRequest>` containing the address of the server to add.
    ///
    /// # Returns
    ///
    /// * `Response<AddServerResponse>` - A response indicating the result of the operation.
    async fn add_server(
        &self,
        request: Request<AddServerRequest>,
    ) -> Result<Response<AddServerResponse>, Status> {
        let req = request.into_inner();
        let mut nodes = self.nodes.lock().await;
        match req.addr.parse::<SocketAddr>() {
            Ok(socket) => {
                info!("Adding server: {:?}", socket);
                nodes.insert(socket.clone());
                Ok(Response::new(AddServerResponse {
                    result: format!("Added {} to cluster", socket),
                }))
            }
            Err(e) => {
                error!("Failed to add server: {}", e);
                return Err(Status::invalid_argument(format!(
                    "Can't add socket to cluster: {e}"
                )));
            }
        }
    }

    /// Retrieves the list of servers in the cluster.
    ///
    /// # Arguments
    ///
    /// * `request` - A `Request<GetServerRequest>`.
    ///
    /// # Returns
    ///
    /// * `Response<GetServerResponse>` - A response containing the list of server addresses.
    async fn get_server(
        &self,
        _request: Request<GetServerRequest>,
    ) -> Result<Response<GetServerResponse>, Status> {
        let mut nodes = self.nodes.lock().await;
        let servers_strings: Vec<String> = nodes.iter().map(|addr| addr.to_string()).collect();
        info!("Retrieved servers: {:?}", servers_strings);
        Ok(Response::new(GetServerResponse {
            result: servers_strings,
        }))
    }
}

impl ImplNodeGroupRpc {
    /// Pings all nodes in the cluster asynchronously.
    async fn ping_nodes(&self) {
        let nodes = self.nodes.lock().await;
        info!("Pinging node(s): {:?}", nodes);

        for node in nodes.iter().cloned() {
            let nodes_clone = self.nodes.clone();
            tokio::spawn(Self::ping_node(nodes_clone, node));
        }
    }

    /// Pings a single node and removes it from the cluster if it doesn't respond.
    ///
    /// # Arguments
    ///
    /// * `nodes` - A reference to the shared set of nodes.
    /// * `node` - The node to ping.
    async fn ping_node(nodes: Arc<Mutex<HashSet<Node>>>, node: Node) {
        let uri = match Uri::builder()
            .scheme("http")
            .authority(node.to_string())
            .path_and_query("/")
            .build()
        {
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

    /// Removes a node from the cluster.
    ///
    /// # Arguments
    ///
    /// * `nodes` - A reference to the shared set of nodes.
    /// * `node` - The node to remove.
    async fn remove_node(nodes: &Arc<Mutex<HashSet<Node>>>, node: &Node) {
        let mut nodes = nodes.lock().await;
        nodes.remove(node);
        info!("Removed node: {}", node);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the logger with the default level set to 'info'.
    // Initialize the logger with the default level set to 'info'.
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

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
    Server::builder()
        .add_service(NodeGroupRpcServer::new(node_group_rpc))
        .serve(addr)
        .await?;

    Ok(())
}
