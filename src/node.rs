//! Node module for a distributed key-value store
//!
//! This module implements a node in a distributed system, providing RPC endpoints
//! for setting and getting key-value pairs, as well as functionality for attaching
//! to a node group for replication.

#[path = "node_group.rs"]
pub mod node_group;

#[path = "node_group_rpc.rs"]
pub mod node_group_rpc;

#[path = "node_rpc.rs"]
pub mod node_rpc;

#[path = "rhm.rs"]
pub mod rhm;

#[path = "storage.rs"]
pub mod storage;

#[path = "utils.rs"]
pub mod utils;

use crate::node_group_rpc::node_group_rpc_client::NodeGroupRpcClient;
use crate::node_group_rpc::node_group_rpc_client::NodeGroupRpcClient as NGClient;
use crate::node_group_rpc::{AddServerRequest, GetServerRequest, ReplicateRequest};
use crate::node_rpc::node_rpc_server::{NodeRpc, NodeRpcServer};
use crate::node_rpc::{GetRequest, GetResponse, PingRequest, PingResponse, SetRequest, SetResponse};
use crate::rhm::{Rhm, RhmResult};
use log::{error, info, debug, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};
use crate::utils::get_endpoint;

/// Custom error type for RHM operations
pub type RhmError = Box<dyn std::error::Error>;

/// Command-line options for the node
#[derive(StructOpt, Debug)]
#[structopt(name = "Node")]
struct Opt {
    /// Address to listen on
    #[structopt(long, parse(try_from_str), default_value = "127.0.0.1:6000")]
    listen: SocketAddr,

    /// Address of the node group (optional)
    #[structopt(long, parse(try_from_str))]
    ng: Option<SocketAddr>,
}

/// Implementation of the NodeRpc trait
#[derive(Debug)]
pub struct ImplNodeRpc {
    rhm: Arc<Mutex<Rhm>>,
    addr: SocketAddr,
    pub ng: Option<Endpoint>,
}

#[tonic::async_trait]
impl NodeRpc for ImplNodeRpc {
    /// Handle a set request
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        let mut rhm = self.rhm.lock().await;
        info!("Received set request: key = {}, value = {}", req.key, req.value);
        let result = rhm.set(&req.key, &req.value).await.map_err(|e| {
            error!("Failed to set value: {}", e);
            Status::internal(format!("Failed to set value: {}", e))
        })?;
        info!("Set request successful: key = {}", req.key);

        // If replication is false, it means the message came from a client, not the node group
        // In this case, we send it to be replicated
        if req.replication || self.ng.is_none() {
            debug!("Skipping replication for key: {}", req.key);
            return Ok(Response::new(SetResponse { result: result.value() }));
        }

        info!("Sending request for replication: key = {}, value = {}", &req.key, &req.value);
        match self.ng().await.ok() {
            None => {
                warn!("Can't replicate: NodeGroup not available");
                error!("Can't replicate")
            },
            Some(mut client) => {
                debug!("Sending replication request to NodeGroup");
                client
                    .replicate(ReplicateRequest {
                        key: req.key.clone(),
                        value: req.value.clone(),
                        source: self.addr.to_string(),
                    })
                    .await?;
                info!("Replication request sent successfully for key: {}", req.key);
            }
        }

        Ok(Response::new(SetResponse { result: result.value() }))
    }

    /// Handle a get request
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let rhm = self.rhm.lock().await;
        info!("Received get request: key = {}", req.key);
        let result = rhm.get(&req.key).await;

        let found = matches!(result, RhmResult::Value(_));
        info!("Get request result: key = {}, found = {}", req.key, found);
        Ok(Response::new(GetResponse {
            value: result.value(),
            found,
        }))
    }

    /// Handle a ping request
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        info!("Received ping request");
        Ok(Response::new(PingResponse { result: "Pong".to_string() }))
    }
}

impl ImplNodeRpc {
    /// Get a connection to the NodeGroup
    async fn ng(&self) -> Result<NodeGroupRpcClient<Channel>, RhmError> {
        let ng = self.ng.clone();
        let ng = ng.ok_or("No NodeGroup found")?;
        debug!("Connecting to NodeGroup at {:?}", ng);
        NGClient::connect(ng).await.map_err(Into::into)
    }

    /// Attach this node to the NodeGroup
    pub async fn attach_to_group(&self) -> Result<(), RhmError> {
        info!("Attaching to NodeGroup");
        let mut client = self.ng().await?;
        client.add_server(AddServerRequest { addr: self.addr.to_string() }).await?;
        let response = client.get_server(GetServerRequest {}).await?;
        info!("Attached to group: {:?}", response);
        Ok(())
    }

    /// Create a new ImplNodeRpc instance
    pub fn new(rhm: Rhm, addr: SocketAddr) -> Self {
        debug!("Creating new ImplNodeRpc instance");
        Self {
            rhm: Arc::new(Mutex::new(rhm)),
            addr,
            ng: None,
        }
    }
}

/// Main function to run the node
#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), RhmError> {
    env_logger::Builder::from_default_env().filter_level(log::LevelFilter::Info).init();

    let opt = Opt::from_args();
    let addr = opt.listen;
    info!("Initializing node with address: {}", addr);
    let rhm = Rhm::new(&addr.to_string()).await?;
    let mut node_rpc = ImplNodeRpc::new(rhm, addr);

    if let Some(ng_addr) = opt.ng {
        info!("NodeGroup address provided: {}", ng_addr);
        let endpoint = get_endpoint(&ng_addr.to_string())?;
        node_rpc.ng = Some(endpoint.clone());
        node_rpc.attach_to_group().await?;
    } else {
        warn!("No NodeGroup address provided. Running in standalone mode.");
    }

    info!("Node listening on {}", addr);

    Server::builder().add_service(NodeRpcServer::new(node_rpc)).serve(addr).await?;

    Ok(())
}