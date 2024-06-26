use remote_hash_map::common::*;

pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../../proto/node_rpc_descriptor.bin");

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("RHM operation failed: {0}")]
    RhmError(String),
    #[error("Node group communication failed: {0}")]
    NodeGroupError(String),
    #[error("Tonic transport error: {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[error("Internal error: {0}")]
    InternalError(String),
}

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

        if req.replication || self.ng.is_none() {
            debug!("Skipping replication for key: {}", req.key);
            return Ok(Response::new(SetResponse { result: result.value() }));
        }

        info!("Sending request for replication: key = {}, value = {}", &req.key, &req.value);
        match self.ng().await {
            Ok(mut client) => {
                debug!("Sending replication request to NodeGroup");
                client
                    .replicate(ReplicateRequest {
                        key: req.key.clone(),
                        value: req.value.clone(),
                        source: self.addr.to_string(),
                    })
                    .await
                    .map_err(|e| Status::internal(format!("Replication failed: {}", e)))?;
                info!("Replication request sent successfully for key: {}", req.key);
            }
            Err(e) => {
                warn!("Can't replicate: NodeGroup not available: {}", e);
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
        Ok(Response::new(GetResponse { value: result.value(), found }))
    }

    /// Handle a ping request
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        info!("Received ping request from: {:?}", request.into_inner());
        Ok(Response::new(PingResponse { result: "Pong".to_string() }))
    }
}

impl ImplNodeRpc {
    /// Get a connection to the NodeGroup
    async fn ng(&self) -> Result<NodeGroupRpcClient<Channel>, NodeError> {
        let ng = self.ng.clone();
        let ng = ng.ok_or(NodeError::NodeGroupError("No NodeGroup found".to_string()))?;
        debug!("Connecting to NodeGroup at {:?}", ng);
        NGClient::connect(ng).await.map_err(NodeError::TonicError)
    }

    /// Attach this node to the NodeGroup
    pub async fn attach_to_group(&self) -> Result<(), NodeError> {
        info!("Attaching to NodeGroup");
        let mut client = self.ng().await?;
        client
            .add_server(AddServerRequest { addr: self.addr.to_string() })
            .await
            .map_err(|e| NodeError::NodeGroupError(e.to_string()))?;
        let response = client.get_server(GetServerRequest {}).await.map_err(|e| NodeError::NodeGroupError(e.to_string()))?;
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
async fn main() -> Result<(), NodeError> {
    env_logger::Builder::from_default_env().filter_level(log::LevelFilter::Info).init();

    let opt = Opt::from_args();
    let addr = opt.listen;
    info!("Initializing node with address: {}", addr);
    let rhm = Rhm::new(&addr.to_string()).await.map_err(|e| NodeError::RhmError(e.to_string()))?;
    let mut node_rpc = ImplNodeRpc::new(rhm, addr);

    if let Some(ng_addr) = opt.ng {
        info!("NodeGroup address provided: {}", ng_addr);
        let endpoint = get_endpoint(&ng_addr.to_string()).map_err(|e| NodeError::InternalError(e.to_string()))?;
        node_rpc.ng = Some(endpoint.clone());
        node_rpc.attach_to_group().await?;
    } else {
        warn!("No NodeGroup address provided. Running in standalone mode.");
    }

    info!("Node listening on {}", addr);

    let reflection_service = match ReflectionBuilder::configure().register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET).build() {
        Ok(service) => service,
        Err(e) => {
            eprintln!("Failed to build reflection service: {}", e);
            panic!()
        }
    };

    Server::builder()
        .add_service(NodeRpcServer::new(node_rpc))
        .add_service(reflection_service)
        .serve(addr)
        .await
        .map_err(NodeError::TonicError)?;

    Ok(())
}
