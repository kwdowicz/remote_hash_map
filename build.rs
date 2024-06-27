// fn main() {
//     tonic_build::compile_protos("proto/node_group_rpc.proto").unwrap();
//     tonic_build::compile_protos("proto/node_rpc.proto").unwrap();
// }
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .file_descriptor_set_path("proto/node_group_rpc_descriptor.bin")
        .compile(&["proto/node_group_rpc.proto"], &["proto"])?;

    tonic_build::configure()
        .build_server(true)
        .file_descriptor_set_path("proto/node_rpc_descriptor.bin")
        .compile(&["proto/node_rpc.proto"], &["proto"])?;

    Ok(())
}
