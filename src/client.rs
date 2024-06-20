#[macro_export]
macro_rules! rhm {
    ($var_name:ident, $ip:expr, $port:expr) => {
        let mut $var_name = match Client::connect(&format!("{}:{}", $ip, $port)).await {
            Ok(client) => client,
            Err(e) => {
                eprintln!("Failed to connect to {}:{}: {}", $ip, $port, e);
                return Err(e.into());
            }
        };
    };
    ($var_name:ident, $address:expr) => {
        let mut $var_name = match Client::connect($address).await {
            Ok(client) => client,
            Err(e) => {
                eprintln!("Failed to connect to {}: {}", $address, e);
                return Err(e.into());
            }
        };
    };
}

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
use tonic::transport::{Channel};
use tonic::Request;
use crate::utils::get_endpoint;

pub type RhmError = Box<dyn std::error::Error>;

pub struct Client {
    node_client: NodeRpcClient<Channel>,
}


impl Client {
    pub async fn connect(ng_addr: &str) -> Result<Self, RhmError> {
        let node_address = get_node_address(ng_addr).await?;
        let node_client = NodeRpcClient::connect(node_address).await?;
        Ok(Self {
            node_client,
        })
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

#[warn(dead_code)]
async fn get_node_address(node_group_addr: &str) -> Result<String, RhmError> {
    let endpoint = get_endpoint(node_group_addr)?;

    let mut ng = NodeGroupRpcClient::connect(endpoint).await?;

    let response = ng.get_server(Request::new(GetServerRequest {})).await?;
    let servers = response.into_inner().result;

    if servers.is_empty() {
        Err("No nodes found".into())
    } else {
        Ok(format!("http://{}", servers[0]))
    }
}