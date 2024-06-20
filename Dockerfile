# Use the official Rust image as the base image
FROM rust:latest

# Install dependencies including protoc and protobuf-compiler
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install protoc-rust plugin from the protobuf crate
RUN cargo install --locked protobuf-codegen

# Set the working directory inside the container
WORKDIR /usr/src/app

# Add protoc-gen-rust to the system PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# Verify the installation
RUN which protoc-gen-rust && protoc-gen-rust --version

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Copy the source code
COPY src ./src

# Copy the .proto files and any related files
COPY proto ./proto

# Copy the build script
COPY build.rs ./

# Set necessary environment variables
ENV OUT_DIR=/usr/src/app/target

# Create output directory
RUN mkdir -p $OUT_DIR

# Build the proto files using protobuf-codegen
RUN protoc --proto_path=proto --rust_out=$OUT_DIR proto/*.proto

# Build the Rust project
RUN cargo build --release

# Expose the port 6000 and 5000
EXPOSE 6000 5000 5001 5002 5003 5004

# Run the compiled binary with the specified arguments
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
