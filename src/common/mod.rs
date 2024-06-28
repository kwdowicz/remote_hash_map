// Local modules
pub mod client;
pub mod utils;

// From crate
pub use crate::{
    common::utils::get_endpoint,
    rhm::{
        rhm::{Rhm, RhmResult},
        storage::Storage,
    },
    rpc::{
        node_group_rpc::{
            node_group_rpc_client::NodeGroupRpcClient,
            node_group_rpc_server::{NodeGroupRpc, NodeGroupRpcServer},
            AddServerRequest, AddServerResponse, GetServerRequest, GetServerResponse, ReplicateRequest, ReplicateResponse,
        },
        node_rpc::{
            node_rpc_client::NodeRpcClient as NClient,
            node_rpc_server::{NodeRpc, NodeRpcServer},
            GetRequest, GetResponse, PingRequest, PingResponse, SetRequest, SetResponse,
        },
    },
};

// External crates
pub use async_trait::async_trait;
pub use http::Uri;
pub use log::{debug, error, info, warn};
pub use structopt::StructOpt;
pub use thiserror::Error;
pub use tokio::{
    fs::{metadata, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt, Result as TResult},
    sync::Mutex,
    time::sleep,
};
pub use tonic::{
    transport::{Channel, Endpoint, Server},
    Request, Response, Status,
};
pub use tonic_reflection::server::Builder as ReflectionBuilder;

// Standard library
pub use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    io,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

// Type aliases
pub use crate::rpc::node_group_rpc::node_group_rpc_client::NodeGroupRpcClient as NGClient;

// Utility functions
pub use crate::common::utils::data_file;