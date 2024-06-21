# Remote Hash Map

A distributed node cluster management system using gRPC, implementing a replicated key-value store.

## Table of Contents

- [Features](#F)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
  - [Running the Node Group](#running-the-node-group)
  - [Running Individual Nodes](#running-individual-nodes)
  - [Using the Client](#using-the-client)
- [Docker Support](#docker-support)
- [Configuration](#configuration)
- [Logging](#logging)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Features

- Distributed key-value storage
- Node cluster management
- Health checking via regular pinging
- Data replication across nodes
- Configurable ping intervals
- Docker support for easy deployment

## Architecture

The system consists of two main components:

1. **Node**: Individual servers capable of storing key-value pairs and responding to ping requests.
2. **NodeGroup**: A manager for the node cluster, handling node addition, retrieval, and health monitoring.

## Getting Started

### Prerequisites

- Rust (with Cargo package manager)
- protoc (Protocol Buffers compiler) for gRPC
- Docker (optional, for containerized deployment)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/kwdowicz/remote_hash_map.git
   cd remote_hash_map
   ```

2. Build the project:
   ```bash
   cargo build --release
   ```

## Usage

### Running the Node Group

Start the NodeGroup service:

```bash
./target/release/ng --listen 127.0.0.1:5000
```

### Running Individual Nodes

Run nodes and connect them to the NodeGroup:

```bash
./target/release/node --listen 127.0.0.1:6001 --ng 127.0.0.1:5000
./target/release/node --listen 127.0.0.1:6002 --ng 127.0.0.1:5000
```

### Using the Client

1. Create a new project:
   ```bash
   mkdir rhm_test && cd rhm_test
   cargo init
   cargo add remote_hash_map tokio
   ```

2. Add the following to `src/main.rs`:
   ```rust
   use remote_hash_map::RHMClient;

   #[tokio::main]
   async fn main() -> Result<(), Box<dyn std::error::Error>> {
       let mut client = RHMClient::connect("127.0.0.1:5000").await?;

       client.set("name", "Daria").await?;
       client.set("name", "Tosia").await?;
       client.set("name", "Gabi").await?;

       let result = client.get("name").await?;
       println!("Name: {}", result);

       Ok(())
   }
   ```

3. Run the client:
   ```bash
   cargo run
   ```

## Docker Support

Build and run using Docker:

```bash
docker build -t remote_hash_map .
docker network create --subnet=192.168.0.0/16 my_network
docker run --name ng --network my_network --ip 192.168.0.3 remote_hash_map ng
docker run --name node --network my_network --ip 192.168.0.4 -e NG_ADDRESS="192.168.0.3:5000" remote_hash_map node
```

## Configuration

- Ping interval can be configured for the NodeGroup:
  ```bash
  ./target/release/ng --listen 127.0.0.1:5000 --ping-sec 4
  ```

## Logging

The project uses `env_logger`. Set the `RUST_LOG` environment variable for different log levels:

```bash
RUST_LOG=debug ./target/release/node --listen 127.0.0.1:6001 --ng 127.0.0.1:5000
```

## Troubleshooting

- Ensure all dependencies are installed and properly configured.
- Verify that specified ports are not in use or blocked by firewalls.
- Check log output for errors or warnings.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue to discuss proposed changes or improvements.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

