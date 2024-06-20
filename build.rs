mod r#Dockerfile

fn main() {
    tonic_build::compile_protos("proto/node_group_rpc.proto").unwrap();
    tonic_build::compile_protos("proto/node_rpc.proto").unwrap();
}
