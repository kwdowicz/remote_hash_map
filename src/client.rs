pub mod node_group_rpc {
    tonic::include_proto!("node_group_rpc");
}

pub mod node_rpc {
    tonic::include_proto!("node_rpc");
}

use node_group_rpc::node_group_rpc_client::NodeGroupRpcClient;
use node_group_rpc::GetServerRequest;
use node_rpc::node_rpc_client::NodeRpcClient;
use node_rpc::{GetRequest, SetRequest};
use tokio::time::sleep;
use tonic::transport::Channel;
use tonic::Request;
use http::uri::Uri;
use std::time::Duration;
use log::{info, error};

pub struct RemoteHashMapClient {
    node_client: NodeRpcClient<Channel>,
}

impl RemoteHashMapClient {
    pub async fn connect_to_node_group(node_group_addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Connect to NodeGroup and get a list of nodes
        let node_addr = match get_node_address(node_group_addr).await {
            Some(addr) => addr,
            None => {
                error!("No available nodes in the NodeGroup");
                return Err("No available nodes in the NodeGroup".into());
            }
        };

        // Connect to the first available node
        let node_client = NodeRpcClient::connect(node_addr).await?;

        Ok(Self { node_client })
    }

    pub async fn set_value(&mut self, key: &str, value: &str) -> Result<String, Box<dyn std::error::Error>> {
        let request = SetRequest {
            key: key.to_string(),
            value: value.to_string(),
        };

        let response = self.node_client.set(Request::new(request)).await?.into_inner();
        Ok(response.result)
    }

    pub async fn get_value(&mut self, key: &str) -> Result<String, Box<dyn std::error::Error>> {
        let request = GetRequest {
            key: key.to_string(),
        };

        let response = self.node_client.get(Request::new(request)).await?.into_inner();
        Ok(response.value)
    }
}

async fn get_node_address(node_group_addr: &str) -> Option<String> {
    let channel = match Channel::from_shared(node_group_addr.to_string()) {
        Ok(c) => c,
        Err(_) => {
            error!("Failed to create channel for NodeGroup at {}", node_group_addr);
            return None;
        }
    };

    let mut client = match NodeGroupRpcClient::connect(channel).await {
        Ok(c) => c,
        Err(_) => {
            error!("Failed to connect to NodeGroup at {}", node_group_addr);
            return None;
        }
    };

    match client.get_server(Request::new(GetServerRequest {})).await {
        Ok(response) => {
            let servers = response.into_inner().result;
            if !servers.is_empty() {
                info!("Found available nodes: {:?}", servers);
                Some(format!("http://{}", servers[0]))
            } else {
                error!("No nodes available in NodeGroup");
                None
            }
        }
        Err(e) => {
            error!("Failed to get nodes from NodeGroup: {}", e);
            None
        }
    }
}
