pub mod node_group_rpc {
    tonic::include_proto!("node_group_rpc");
}

pub mod node_rpc {
    tonic::include_proto!("node_rpc");
}

use http::uri::Uri;
use log::{error, info};
use node_group_rpc::node_group_rpc_client::NodeGroupRpcClient;
use node_group_rpc::GetServerRequest;
use node_rpc::node_rpc_client::NodeRpcClient;
use node_rpc::{GetRequest, SetRequest};
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

pub type RhmError = Box<dyn std::error::Error>;

pub struct RHMClient {
    node_client: NodeRpcClient<Channel>,
}

impl RHMClient {
    pub async fn connect(node_group_addr: &str) -> Result<Self, RhmError> {
        // Connect to NodeGroup and get a list of nodes
        let node_addr = match get_node_address(node_group_addr).await {
            Some(addr) => addr,
            None => {
                return Err("No available nodes in the NodeGroup".into());
            }
        };

        // Connect to the first available node
        let node_client = NodeRpcClient::connect(node_addr).await?;

        Ok(Self { node_client })
    }

    pub async fn set(&mut self, key: &str, value: &str) -> Result<String, RhmError> {
        let request = SetRequest {
            key: key.to_string(),
            value: value.to_string(),
            replication: false,
        };

        let response = self.node_client.set(Request::new(request)).await?.into_inner();
        Ok(response.result)
    }

    pub async fn get(&mut self, key: &str) -> Result<String, RhmError> {
        let request = GetRequest { key: key.to_string() };

        let response = self.node_client.get(Request::new(request)).await?.into_inner();
        Ok(response.value)
    }
}

async fn get_node_address(node_group_addr: &str) -> Option<String> {
    let uri = Uri::builder().scheme("http").authority(node_group_addr).path_and_query("/").build().expect("Unable to build uri");
    let endpoint = Endpoint::from_shared(uri.to_string()).expect("Unable to build endpoint");

    let mut client = match NodeGroupRpcClient::connect(endpoint).await {
        Ok(c) => c,
        Err(e) => {
            println!("Failed to connect to NodeGroup at {} {}", node_group_addr, e);
            return None;
        }
    };

    match client.get_server(Request::new(GetServerRequest {})).await {
        Ok(response) => {
            let servers = response.into_inner().result;
            if !servers.is_empty() {
                info!("Found available nodes: {:?}", servers);
                // TODO: getting first available server (node) add some logic here
                Some(format!("http://{}", servers[0]))
            } else {
                println!("No nodes available in NodeGroup");
                None
            }
        }
        Err(e) => {
            println!("Failed to get nodes from NodeGroup: {}", e);
            None
        }
    }
}
