pub mod node_group;
pub mod node_group_rpc;
pub mod node_rpc;
pub mod rhm;
pub mod storage;

use crate::node_group_rpc::{AddServerRequest, GetServerRequest, ReplicateRequest};
use crate::rhm::{Rhm, RhmResult};
use log::{error, info};
use node_group_rpc::node_group_rpc_client::NodeGroupRpcClient as NGClient;
use node_rpc::node_rpc_server::{NodeRpc, NodeRpcServer};
use node_rpc::{
    GetRequest, GetResponse, PingRequest, PingResponse, SetRequest, SetResponse,
};
use std::net::SocketAddr;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint, Error, Server, Uri};
use tonic::{Request, Response, Status};
use crate::node_group_rpc::node_group_rpc_client::NodeGroupRpcClient;

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
    ng: Option<Endpoint>,
}

#[tonic::async_trait]
impl NodeRpc for ImplNodeRpc {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        let mut rhm = self.rhm.lock().await;
        info!(
            "Received set request: key = {}, value = {}",
            req.key, req.value
        );
        let result = rhm.set(&req.key, &req.value).await.map_err(|e| {
            error!("Failed to set value: {}", e);
            Status::internal(format!("Failed to set value: {}", e))
        })?;

        info!("Set request successful: key = {}", req.key);
        // TODO: now replicate this to other nodes
        // connect to node group and send a new set command
        match &self.ng {
            None => (),
            Some(ng) => {
                info!("Will replicate: {:?}:{:?}", &req.key, &req.value);
                match self.ng().await.ok() {
                    None => (),
                    Some(mut client) => {
                        client.replicate(ReplicateRequest {
                            key: req.key,
                            value: req.value,
                        }).await?;
                    }
                }
            }
        }

        // Respond with value or status to client
        Ok(Response::new(SetResponse {
            result: result.value(),
        }))
    }

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

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        info!("Received ping request");
        Ok(Response::new(PingResponse {
            result: "Pong".to_string(),
        }))
    }
}

impl ImplNodeRpc {
    async fn ng(&self) -> Result<NodeGroupRpcClient<Channel>, Box<dyn std::error::Error>> {
        let ng = self.ng.clone();
        let ng = ng.ok_or("No NodeGroup found")?;
        // let ng = ng.ok_or("No NodeGroup found")?;
        NGClient::connect(ng).await.map_err(Into::into)
    }

    async fn attach_to_group(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut client = self.ng().await?;
        client
            .add_server(AddServerRequest {
                addr: self.addr.to_string(),
            })
            .await?;
        let response = client.get_server(GetServerRequest {}).await?;
        info!("Attached to group: {:?}", response);
        Ok(())
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let opt = Opt::from_args();
    let addr = opt.listen;
    let mut node_rpc = ImplNodeRpc {
        rhm: Arc::new(Mutex::new(Rhm::new().await?)),
        addr,
        ng: None,
    };

    if let Some(ng_addr) = opt.ng {
        let uri = Uri::builder()
            .scheme("http")
            .authority(ng_addr.to_string())
            .path_and_query("/")
            .build()?;
        let endpoint = Endpoint::from_shared(uri.to_string())?;
        node_rpc.ng = Some(endpoint.clone());
        node_rpc.attach_to_group().await?;
    }

    info!("Node listening on {}", addr);

    // Start the gRPC server.
    Server::builder()
        .add_service(NodeRpcServer::new(node_rpc))
        .serve(addr)
        .await?;

    Ok(())
}
