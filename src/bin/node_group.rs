use remote_hash_map::common::*;

pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../../proto/node_group_rpc_descriptor.bin");

/// Custom error type for ping operations
#[allow(dead_code)]
#[derive(Debug)]
struct PingError(String);

impl fmt::Display for PingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for PingError {}

/// Type alias for a thread-safe set of nodes
type Nodes = Arc<Mutex<HashSet<SocketAddr>>>;

/// Type alias for a single node address
#[allow(dead_code)]
type Node = SocketAddr;

/// Implementation of the NodeGroupRpc trait
#[derive(Debug, Clone)]
pub struct ImplNodeGroupRpc {
    nodes: Nodes,
}

/// Command-line options for the node group
#[derive(StructOpt, Debug)]
#[structopt(name = "NodeGroup")]
struct Opt {
    /// Address to listen on
    #[structopt(long, default_value = "127.0.0.1:5000")]
    #[allow(dead_code)]
    listen: SocketAddr,

    /// Interval for pinging nodes (in seconds)
    #[structopt(long, default_value = "30")]
    #[allow(dead_code)]
    ping_sec: u64,
}

#[tonic::async_trait]
impl NodeGroupRpc for ImplNodeGroupRpc {
    /// Handle a request to add a server to the node group
    async fn add_server(&self, request: Request<AddServerRequest>) -> Result<Response<AddServerResponse>, Status> {
        let req = request.into_inner();
        let mut nodes = self.nodes.lock().await;
        req.addr
            .parse::<SocketAddr>()
            .map(|socket| {
                info!("Adding node: {}", socket);
                nodes.insert(socket);
                Response::new(AddServerResponse {
                    result: format!("Added {} to node group", socket),
                })
                // TODO: Check if other nodes exist if yes then start replicating to them previous messages
            })
            .map_err(|e| {
                error!("Failed to add server: {}", e);
                Status::invalid_argument(format!("Can't add node to node group: {}", e))
            })
    }

    /// Handle a request to get information about servers in the node group
    async fn get_server(&self, _request: Request<GetServerRequest>) -> Result<Response<GetServerResponse>, Status> {
        let nodes = self.nodes.lock().await;
        let servers: Vec<String> = nodes.iter().map(ToString::to_string).collect();
        info!("Retrieved servers: {:?}", servers);
        Ok(Response::new(GetServerResponse { result: servers }))
    }

    /// Handle a replication request
    async fn replicate(&self, request: Request<ReplicateRequest>) -> Result<Response<ReplicateResponse>, Status> {
        info!("Received replication request: {:?}", request);

        let mut nodes = self.nodes.lock().await;
        let request = request.into_inner();
        let source_addr: SocketAddr = request.source.parse().map_err(|_| Status::invalid_argument("Invalid source address"))?;

        let nodes_to_replicate: Vec<String> = nodes.iter().filter(|&addr| *addr != source_addr).map(ToString::to_string).collect();

        info!("Nodes to replicate to: {:?}", nodes_to_replicate);

        for node in &nodes_to_replicate {
            if let Err(e) = self.replicate_to_node(node, &request.key, &request.value).await {
                warn!("Failed to replicate to node {}: {}", node, e);
                if let Ok(socket) = node.parse() {
                    nodes.remove(&socket);
                }
            }
        }

        Ok(Response::new(ReplicateResponse {}))
    }
}

impl ImplNodeGroupRpc {
    /// Ping all nodes in the group
    #[allow(dead_code)]
    async fn ping_nodes(&self) {
        let nodes = self.nodes.lock().await;
        info!("Pinging node(s): {:?}", nodes);

        for node in nodes.iter().cloned() {
            let nodes_clone = self.nodes.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::ping_node(node).await {
                    error!("Failed to ping node {}: {}", node, e);
                    Self::remove_node(&nodes_clone, &node).await;
                }
            });
        }
    }

    /// Ping a single node
    #[allow(dead_code)]
    async fn ping_node(node: Node) -> Result<(), PingError> {
        debug!("Pinging node: {}", node);
        let endpoint = get_endpoint(&node.to_string()).map_err(|e| PingError(e.to_string()))?;
        let mut connection = NClient::connect(endpoint).await.map_err(|e| PingError(e.to_string()))?;
        let response = connection.ping(PingRequest {}).await.map_err(|e| PingError(e.to_string()))?;

        if response.into_inner().result != "Pong" {
            return Err(PingError("Invalid ping response".into()));
        }
        debug!("Ping successful for node: {}", node);
        Ok(())
    }

    /// Remove a node from the group
    #[allow(dead_code)]
    async fn remove_node(nodes: &Nodes, node: &Node) {
        let mut nodes = nodes.lock().await;
        nodes.remove(node);
        info!("Removed node: {}", node);
    }

    /// Replicate data to a specific node
    async fn replicate_to_node(&self, node: &str, key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
        debug!("Replicating to node: {}", node);
        let endpoint = get_endpoint(node)?;
        let mut client = NClient::connect(endpoint).await?;
        client
            .set(SetRequest {
                key: key.to_string(),
                value: value.to_string(),
                replication: true,
            })
            .await?;
        debug!("Replication successful for node: {}", node);
        Ok(())
    }

    /// Create a new ImplNodeGroupRpc instance
    pub fn new() -> Self {
        debug!("Creating new ImplNodeGroupRpc instance");
        Self {
            nodes: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

/// Main function to run the node group
#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_default_env().filter_level(log::LevelFilter::Info).init();

    let opt = Opt::from_args();
    let addr = opt.listen;
    let node_group_rpc = ImplNodeGroupRpc::new();

    let ng_for_pinging = node_group_rpc.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(opt.ping_sec)).await;
            debug!("Starting periodic ping of nodes");
            ng_for_pinging.ping_nodes().await;
        }
    });

    let reflection_service = match ReflectionBuilder::configure().register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET).build() {
        Ok(service) => service,
        Err(e) => {
            eprintln!("Failed to build reflection service: {}", e);
            panic!()
        }
    };

    info!("NodeGroup listening on {}", addr);

    Server::builder()
        .add_service(NodeGroupRpcServer::new(node_group_rpc))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
