# Remote Hash Map

This project implements a distributed node cluster management system using gRPC. The system includes two main components:
1. **Node**: Represents individual nodes in the cluster, capable of storing key-value pairs and responding to ping requests.
2. **NodeGroup**: Manages a group of nodes, handles adding and retrieving nodes, and regularly pings nodes to ensure they are responsive.

## Features

- **Node Management**: Add and retrieve nodes from the cluster.
- **Key-Value Storage**: Store and retrieve key-value pairs in individual nodes.
- **Health Checking**: Regularly ping nodes to check their health and remove unresponsive nodes.

## Getting Started

### Prerequisites

- Rust (with `cargo` package manager)
- `protoc` (Protocol Buffers compiler) for gRPC

### Building the Project

To compile the project, run:

```sh
cargo build --release
```

This will build the project and produce the executables in the `target/release` directory.

### Running the Project

#### Run a Single Node on Default Port

To run a single node using the default port, execute:

```sh
cargo run --bin node
```

#### Run a Node on a Specified Port

To run a node on a specified port, use the `--listen` option:

```sh
cargo run --bin node -- --listen 127.0.0.1:4567
```

#### Use Group (Cluster) Functionality

If you want to use the group (cluster) functionality, follow these steps:

1. **Run NodeGroup**: Start the NodeGroup on a specified port:

    ```sh
    cargo run --bin ng -- --listen 127.0.0.1:6001
    ```

2. **Run Nodes and Attach to the Group**: Start as many nodes as you wish and attach them to the NodeGroup:

    ```sh
    cargo run --bin node -- --listen 127.0.0.1:6677 --ng 127.0.0.1:6001
    cargo run --bin node -- --listen 127.0.0.1:6675 --ng 127.0.0.1:6001
    ```
3. **Pinging is configurable**:
   
   ```sh
   cargo run --bin ng -- --listen 127.0.0.1:6001 --ping-sec 4
   #ping every 4 seconds
   ```

### Detailed Steps

1. **Compile the Project**: Ensure you have Rust and `cargo` installed, then compile the project:

    ```sh
    cargo build --release
    ```

2. **Run NodeGroup**: Start the NodeGroup service to manage the cluster:

    ```sh
    cargo run --bin ng -- --listen 127.0.0.1:6001
    ```

3. **Run Nodes**: Start individual nodes and attach them to the NodeGroup:

    ```sh
    cargo run --bin node -- --listen 127.0.0.1:6677 --ng 127.0.0.1:6001
    cargo run --bin node -- --listen 127.0.0.1:6675 --ng 127.0.0.1:6001
    ```

### Logging

The project uses `env_logger` for logging. By default, the log level is set to `info`. To see the log output, simply run the commands as specified. If you need more detailed logs (e.g., `debug` level), set the `RUST_LOG` environment variable:

```sh
RUST_LOG=debug cargo run --bin node -- --listen 127.0.0.1:4567
```

### Troubleshooting

- Ensure that all necessary dependencies are installed and properly configured.
- Verify that the ports you are using are not blocked by any firewall or used by other services.
- Check the log output for any errors or warnings that might indicate what is going wrong.

### Contributing

Contributions are welcome! Please submit a pull request or open an issue to discuss any changes or improvements.

### License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
```
