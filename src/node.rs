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
use crate::node_group_rpc::{AddServerRequest, GetServerRequest, ReplicateRequest};
use crate::rhm::{Rhm, RhmResult};
use crate::node_group_rpc::node_group_rpc_client::NodeGroupRpcClient as NGClient;
use crate::node_rpc::node_rpc_server::{NodeRpc, NodeRpcServer};
use crate::node_rpc::{GetRequest, GetResponse, PingRequest, PingResponse, SetRequest, SetResponse};
use std::net::SocketAddr;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint, Server, Uri};
use tonic::{Request, Response, Status};
use log::{info, error};


pub type RhmError = Box<dyn std::error::Error>;

#[derive(StructOpt, Debug)]
#[structopt(name = "Node")]
struct Opt {
    #[structopt(long, parse(try_from_str), default_value = "127.0.0.1:6000")]
    listen: SocketAddr,

    #[structopt(long, parse(try_from_str))]
    ng: Option<SocketAddr>,
}

#[derive(Debug)]
pub struct ImplNodeRpc {
    rhm: Arc<Mutex<Rhm>>,
    addr: SocketAddr,
    pub ng: Option<Endpoint>,
}

#[tonic::async_trait]
impl NodeRpc for ImplNodeRpc {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        let mut rhm = self.rhm.lock().await;
        info!("Received set request: key = {}, value = {}", req.key, req.value);
        let result = rhm.set(&req.key, &req.value).await.map_err(|e| {
            error!("Failed to set value: {}", e);
            Status::internal(format!("Failed to set value: {}", e))
        })?;
        info!("Set request successful: key = {}", req.key);

        // if replication is false, means that message came from client, not node group
        // then we are sending it to be replicated
        if req.replication || self.ng.is_none() {
            return Ok(Response::new(SetResponse { result: result.value() }));
        }

        info!("Sending request for replication: {:?}:{:?}", &req.key, &req.value);
        match self.ng().await.ok() {
            None => error!("Can't replicate"),
            Some(mut client) => {
                client
                    .replicate(ReplicateRequest {
                        key: req.key,
                        value: req.value,
                        source: self.addr.to_string(),
                    })
                    .await?;
            }
        }

        // Respond with value or status to client
        Ok(Response::new(SetResponse { result: result.value() }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let rhm = self.rhm.lock().await;
        info!("Received get request: key = {}", req.key);
        let result = rhm.get(&req.key).await;

        info!("Get request result: key = {}, found = {}", req.key, matches!(result, RhmResult::Value(_)));
        Ok(Response::new(GetResponse {
            value: result.value(),
            found: matches!(result, RhmResult::Value(_)),
        }))
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        info!("Received ping request");
        Ok(Response::new(PingResponse { result: "Pong".to_string() }))
    }
}

impl ImplNodeRpc {
    async fn ng(&self) -> Result<NodeGroupRpcClient<Channel>, RhmError> {
        let ng = self.ng.clone();
        let ng = ng.ok_or("No NodeGroup found")?;
        NGClient::connect(ng).await.map_err(Into::into)
    }

    pub async fn attach_to_group(&self) -> Result<(), RhmError> {
        let mut client = self.ng().await?;
        client.add_server(AddServerRequest { addr: self.addr.to_string() }).await?;
        let response = client.get_server(GetServerRequest {}).await?;
        info!("Attached to group: {:?}", response);
        Ok(())
    }
    
    pub fn new(rhm: Rhm, addr: SocketAddr) -> Self {
        Self {
            rhm: Arc::new(Mutex::new(rhm)),
            addr,
            ng: None,
        }
    }
}

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), RhmError> {
    env_logger::Builder::from_default_env().filter_level(log::LevelFilter::Info).init();

    let opt = Opt::from_args();
    let addr = opt.listen;
    let rhm = Rhm::new(&addr.to_string()).await?;
    let mut node_rpc = ImplNodeRpc::new(rhm, addr);

    if let Some(ng_addr) = opt.ng {
        let uri = Uri::builder().scheme("http").authority(ng_addr.to_string()).path_and_query("/").build()?;
        let endpoint = Endpoint::from_shared(uri.to_string())?;
        node_rpc.ng = Some(endpoint.clone());
        node_rpc.attach_to_group().await?;
    }

    info!("Node listening on {}", addr);

    // Start the gRPC server.
    Server::builder().add_service(NodeRpcServer::new(node_rpc)).serve(addr).await?;

    Ok(())
}
