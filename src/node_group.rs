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
        let mut result = String::from("Unknown");
        match req.addr.parse::<SocketAddr>() {
            Ok(socket) => { servers.insert(socket.clone()); result = format!("Added: {} to cluster", socket); },
            Err(e) => result = format!("Can't add socket to cluster: {e}"),
        }

        println!("{:#?}", servers);

        Ok(Response::new(AddServerResponse {
            result,
        }))
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

