pub mod rhm;
pub mod node_group;
pub mod storage;
pub mod node_group_rpc;
pub mod node_rpc;

use crate::rhm::{Rhm, RhmResult};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tonic::transport::{Endpoint, Server, Uri};
use structopt::StructOpt;
use node_rpc::{GetRequest, GetResponse, SetRequest, SetResponse, PingRequest, PingResponse};
use node_rpc::node_rpc_server::{NodeRpc, NodeRpcServer};
use node_group_rpc::node_group_rpc_client::NodeGroupRpcClient as NGClient;
use crate::node_group_rpc::{AddServerRequest, GetServerRequest};

#[derive(StructOpt, Debug)]
#[structopt(name = "Node")]
struct Opt {
    #[structopt(long, parse(try_from_str), default_value="127.0.0.1:6000")]
    listen: SocketAddr,

    #[structopt(long, parse(try_from_str))]
    ng: Option<SocketAddr>,
}

#[derive(Debug)]
pub struct ImplNodeRpc {
    rhm: Arc<Mutex<Rhm>>,
    addr: SocketAddr,
}

#[tonic::async_trait]
impl NodeRpc for ImplNodeRpc {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        let mut rhm = self.rhm.lock().await;
        let result = rhm.set(&req.key, &req.value).await.map_err(|e| Status::internal(format!("Failed to set value: {}", e)))?;

        Ok(Response::new(SetResponse {
            result: result.value(),
        }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let mut rhm = self.rhm.lock().await;
        let result = rhm.get(&req.key).await;

        Ok(Response::new(
            GetResponse {
                value: result.value(),
                found: matches!(result, RhmResult::Value(_))
            }
        ))
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(
            PingResponse {
                result: "Pong".to_string(),
            }
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();
    let addr = opt.listen;
    let node_rpc = ImplNodeRpc {
        rhm: Arc::new(Mutex::new(Rhm::new().await?)),
        addr,
    };
    println!("{:#?}", node_rpc);

    match opt.ng {
        Some(ng_addr) => attach_to_group(ng_addr, node_rpc.addr).await?,
        None => ()
    }

    println!("Node listening on {}", addr);

    Server::builder()
        .add_service(NodeRpcServer::new(node_rpc))
        .serve(addr)
        .await?;

    Ok(())
}

async fn attach_to_group(ng_addr: SocketAddr, node_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(ng_addr.to_string())
        .path_and_query("/")
        .build()?;
    let endpoint = Endpoint::from_shared(uri.to_string())?;
    let mut client = NGClient::connect(endpoint).await?;
    client.add_server(AddServerRequest { addr: node_addr.to_string() }).await?;
    let response = client.get_server(GetServerRequest{}).await?;
    println!("{:?}", response);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::time::sleep;
    use super::*;
    use crate::rhm::Rhm;

    pub struct TestNodeRpc {
        rhm: Arc<Mutex<Rhm>>,
        addr: SocketAddr,
    }

    #[tonic::async_trait]
    impl NodeRpc for TestNodeRpc {
        async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
            let req = request.into_inner();
            let mut rhm = self.rhm.lock().await;
            let result = rhm.set(&req.key, &req.value).await.map_err(|e| Status::internal(format!("Failed to set value: {}", e)))?;

            Ok(Response::new(SetResponse {
                result: result.value(),
            }))
        }

        async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
            let req = request.into_inner();
            let mut rhm = self.rhm.lock().await;
            let result = rhm.get(&req.key).await;

            Ok(Response::new(
                GetResponse {
                    value: result.value(),
                    found: matches!(result, RhmResult::Value(_))
                }
            ))
        }

        async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
            Ok(Response::new(
                PingResponse {
                    result: "Pong".to_string(),
                }
            ))
        }
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let rhm = Arc::new(Mutex::new(Rhm::new().await.unwrap()));
        let addr = "127.0.0.1:6001".parse().unwrap();
        let node_rpc = TestNodeRpc { rhm: rhm.clone(), addr };

        let svc = NodeRpcServer::new(node_rpc);

        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(3)).await;

        let mut client = crate::node_rpc::node_rpc_client::NodeRpcClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let set_request = tonic::Request::new(SetRequest {
            key: "key1".into(),
            value: "value1".into(),
        });
        let set_response = client.set(set_request).await.unwrap().into_inner();
        assert_eq!(set_response.result, "value1");

        let get_request = tonic::Request::new(GetRequest {
            key: "key1".into(),
        });
        let get_response = client.get(get_request).await.unwrap().into_inner();
        assert_eq!(get_response.value, "value1");
    }

    #[tokio::test]
    async fn test_ping() {
        let rhm = Arc::new(Mutex::new(Rhm::new().await.unwrap()));
        let addr = "127.0.0.1:6002".parse().unwrap();
        let node_rpc = TestNodeRpc { rhm: rhm.clone(), addr };

        let svc = NodeRpcServer::new(node_rpc);

        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(3)).await;

        let mut client = crate::node_rpc::node_rpc_client::NodeRpcClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let ping_request = tonic::Request::new(PingRequest {});
        let ping_response = client.ping(ping_request).await.unwrap().into_inner();
        assert_eq!(ping_response.result, "Pong");
    }
}