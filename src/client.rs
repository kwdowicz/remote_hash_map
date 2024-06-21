//! RHM (Remote Hash Map) Client Module
//!
//! This module provides a client interface for interacting with a distributed key-value store.
//! It includes a macro for easy client creation and structs for RPC communication.

use log::{error, info, debug};
use tonic::transport::Channel;
use tonic::Request;
use crate::utils::get_endpoint;

/// Macro for creating a new RHM client
#[macro_export]
macro_rules! rhm {
    ($var_name:ident, $ip:expr, $port:expr) => {
        let mut $var_name = match Client::connect(&format!("{}:{}", $ip, $port)).await {
            Ok(client) => {
                info!("Successfully connected to {}:{}", $ip, $port);
                client
            },
            Err(e) => {
                error!("Failed to connect to {}:{}: {}", $ip, $port, e);
                return Err(e.into());
            }
        };
    };
    ($var_name:ident, $address:expr) => {
        let mut $var_name = match Client::connect($address).await {
            Ok(client) => {
                info!("Successfully connected to {}", $address);
                client
            },
            Err(e) => {
                error!("Failed to connect to {}: {}", $address, e);
                return Err(e.into());
            }
        };
    };
}

/// Module for node group RPC definitions
pub mod node_group_rpc {
    tonic::include_proto!("node_group_rpc");
}

/// Module for node RPC definitions
pub mod node_rpc {
    tonic::include_proto!("node_rpc");
}

use node_group_rpc::node_group_rpc_client::NodeGroupRpcClient;
use node_group_rpc::GetServerRequest;
use node_rpc::node_rpc_client::NodeRpcClient;
use node_rpc::{GetRequest, SetRequest};

/// Custom error type for RHM operations
pub type RhmError = Box<dyn std::error::Error>;

/// Client struct for interacting with the distributed key-value store
pub struct Client {
    node_client: NodeRpcClient<Channel>,
}

impl Client {
    /// Create a new client connection
    pub async fn connect(ng_addr: &str) -> Result<Self, RhmError> {
        debug!("Attempting to connect to node group at {}", ng_addr);
        let node_address = get_node_address(ng_addr).await?;
        info!("Connecting to node at {}", node_address);
        let node_client = NodeRpcClient::connect(node_address).await?;
        Ok(Self {
            node_client,
        })
    }

    /// Set a key-value pair in the store
    pub async fn set(&mut self, key: &str, value: &str) -> Result<String, RhmError> {
        debug!("Setting key '{}' with value '{}'", key, value);
        let request = SetRequest {
            key: key.to_string(),
            value: value.to_string(),
            replication: false,
        };

        let response = self.node_client.set(Request::new(request)).await?.into_inner();
        info!("Successfully set key '{}' with result: {}", key, response.result);
        Ok(response.result)
    }

    /// Get the value for a given key from the store
    pub async fn get(&mut self, key: &str) -> Result<String, RhmError> {
        debug!("Getting value for key '{}'", key);
        let request = GetRequest { key: key.to_string() };

        let response = self.node_client.get(Request::new(request)).await?.into_inner();
        info!("Successfully retrieved value for key '{}'", key);
        Ok(response.value)
    }
}

/// Get the address of a node from the node group
#[warn(dead_code)]
async fn get_node_address(node_group_addr: &str) -> Result<String, RhmError> {
    debug!("Getting node address from node group at {}", node_group_addr);
    let endpoint = get_endpoint(node_group_addr)?;

    let mut ng = NodeGroupRpcClient::connect(endpoint).await?;

    let response = ng.get_server(Request::new(GetServerRequest {})).await?;
    let servers = response.into_inner().result;

    if servers.is_empty() {
        error!("No nodes found in the node group");
        Err("No nodes found".into())
    } else {
        let node_address = format!("http://{}", servers[0]);
        info!("Selected node address: {}", node_address);
        Ok(node_address)
    }
}