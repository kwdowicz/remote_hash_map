// tests/end_to_end.rs

use tokio::sync::oneshot;
use tonic::transport::{Endpoint, Server};
use remote_hash_map::node::ImplNodeRpc;
use remote_hash_map::node_group::ImplNodeGroupRpc;
use remote_hash_map::node_rpc::node_rpc_server::NodeRpcServer;
use remote_hash_map::node_group_rpc::node_group_rpc_server::NodeGroupRpcServer;
use remote_hash_map::rhm::Rhm;
use remote_hash_map::utils::data_file;
use std::net::SocketAddr;
use std::fs;
use http::Uri;

async fn create_node(node_ip_port: &str, ng_ip_port: &str) -> ImplNodeRpc {
    let node_addr: SocketAddr = node_ip_port.parse().unwrap();
    let rhm = Rhm::new(&node_addr.to_string()).await.unwrap();
    let mut node_rpc = ImplNodeRpc::new(rhm, node_addr);
    let uri = Uri::builder().scheme("http").authority(ng_ip_port.to_string()).path_and_query("/").build().unwrap();
    let endpoint = Endpoint::from_shared(uri.to_string()).unwrap();
    node_rpc.ng = Some(endpoint.clone());
    node_rpc.attach_to_group().await.unwrap();
    node_rpc
}

#[tokio::test]
async fn test_end_to_end() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Start NodeGroup server
    let ng_ip_port = "127.0.0.1:5000";
    let node1_ip_port = "127.0.0.1:6001";
    let node2_ip_port = "127.0.0.1:6002";

    let node_group_addr: SocketAddr = ng_ip_port.parse().unwrap();
    let node_group_rpc = ImplNodeGroupRpc::new();

    let (node_group_tx, node_group_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeGroupRpcServer::new(node_group_rpc))
            .serve_with_shutdown(node_group_addr, async {
                node_group_rx.await.ok();
            })
            .await
            .unwrap();
    });

    // Allow the server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Start First Node server
    let node1_rpc = create_node(node1_ip_port, ng_ip_port).await;

    let (node1_tx, node1_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeRpcServer::new(node1_rpc))
            .serve_with_shutdown(node1_ip_port.parse().unwrap(), async {
                node1_rx.await.ok();
            })
            .await
            .unwrap();
    });

    // Allow the server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Start Second Node server
    let node2_rpc = create_node(node2_ip_port, ng_ip_port).await;

    let (node2_tx, node2_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeRpcServer::new(node2_rpc))
            .serve_with_shutdown(node2_ip_port.parse().unwrap(), async {
                node2_rx.await.ok();
            })
            .await
            .unwrap();
    });

    // Allow the server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Test client
    let mut client = remote_hash_map::RHMClient::connect(&format!("{}", ng_ip_port)).await.unwrap();

    // Test setting a fresh value
    let key = "test_key";
    let value = "test_value";
    let set_result = client.set(key, value).await.unwrap();
    assert_eq!(set_result, "Ok".to_string());

    // Test changing a value
    let key = "test_key";
    let value = "test_value";
    let set_result = client.set(key, value).await.unwrap();
    assert_eq!(set_result, value);

    // Test getting the value
    let get_result = client.get(key).await.unwrap();
    assert_eq!(get_result, value);

    // Test replication
    let node1_file = fs::read(data_file(node1_ip_port)).unwrap();
    let node2_file = fs::read(data_file(node2_ip_port)).unwrap();
    assert_eq!(node1_file, node2_file, "File contents do not match");

    // Shutdown one node
    node1_tx.send(()).unwrap();

    // Test if removed from the group
    // TODO

    // Shutdown rest
    node2_tx.send(()).unwrap();
    node_group_tx.send(()).unwrap();

    // Delete the storage file
    fs::remove_file(data_file(node1_ip_port)).unwrap();
    fs::remove_file(data_file(node2_ip_port)).unwrap();
}
