fn main() {
    tonic_build::compile_protos("proto/node_group_rpc.proto").unwrap();
}